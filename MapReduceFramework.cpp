#include <cstdlib>
#include <cstdio>
#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <iostream>
#include <algorithm>    // std::sort
#include <semaphore.h>

#define FIRSTTHREAD 0

typedef struct ThreadContext{
    unsigned int threadId;
    unsigned int MT;
    Barrier * barrier;
    std::atomic<int>* atomicIndex;
    std::atomic<int> *outAtomicIndex;
    std::atomic<int> *reduceAtomic;
    const InputVec* inputVec;
    OutputVec* outputVec;
    std::vector<IntermediateVec> *arrayOfInterVec;
    const MapReduceClient* client;
    std::vector <IntermediateVec> *Queue;
    sem_t *mutexQueue;
    sem_t *fillCount;
}ThreadContext;

typedef struct MapContext{
    IntermediateVec *interVector;
}MapContext;

typedef struct ReduceContext{
    OutputVec *outVector;
    std::atomic<int> *outAtomicIndex;
}ReduceContext;

void safeExit(ThreadContext* tc, bool err_exit)
{
    if(tc == nullptr){
        exit(-1);
    }
    tc->Queue->clear();
    if(sem_destroy(tc->fillCount) != 0){
        std::cerr<<"sem_destory"<<std::endl;
    }
    if(sem_destroy(tc->mutexQueue) != 0){
        std::cerr<<"sem_destory"<<std::endl;
    }
    if (err_exit){
        exit(-1);
    }
}

void printErr(std::string msg, ThreadContext *tc){
    std::cerr<<msg<<std::endl;
    safeExit(tc, true);
}

//for debug
//void printInterVector(IntermediateVec** array, int numOfThreads){
//    printf("++++++++++++\n");
//    for (int i = 1; i < numOfThreads; ++i){
//        printf("thread id: %d:\n",i);
//        for (unsigned long j = 0; j < (*(array[i])).size() ; ++j){
//            //printIntermediatePair((&(*(array[i])).at(j)));
//        }
//        printf("~~~~~~~~\n");
//    }
//}

void emit2 (K2* key, V2* value, void* context){
    auto* tc = (MapContext*) context;
    tc->interVector->emplace_back(key, value);
}
void emit3 (K3* key, V3* value, void* context){
    auto* tc = (ReduceContext*) context;
    int oldValue = (*(tc->outAtomicIndex))++ ;
    auto iterator = (*(tc->outVector)).begin();
    (*(tc->outVector)).emplace(iterator+(oldValue), key, value);
}

bool comperator(IntermediatePair &p1, IntermediatePair &p2){
    return (*p1.first < *p2.first);
}

// check the IntermediateVec isn't empty, in case it doesn't find the next max key
bool check_empty_find_max(std::vector<IntermediateVec> *arr, int MT, K2 **max){
    bool isEmpty = true;
    for (int i = FIRSTTHREAD; i < MT; ++i) {
        if (not((arr)->at(i)).empty()) {
            isEmpty = false;
            if(*max == nullptr){
                *max = ((arr)->at(i)).back().first;
            }
            else{
                if((**max) < *((arr)->at(i)).back().first)
                {
                    *max = ((arr)->at(i)).back().first;
                }
            }
        }
    }
    return isEmpty;
}

bool is_eq(K2 *max, IntermediatePair &p){
    if(not(*max < *p.first)){
        if(not(*p.first < *max)){
            return true;
        }
    }
    return false;
}

void* threadLogic (void* context) {
    auto *tc = (ThreadContext *) context;
    unsigned int oldValue = (*(tc->atomicIndex))++;

    //map logic
    while (oldValue < tc->inputVec->size()) {
        auto k1 = tc->inputVec->at(oldValue).first;
        auto v1 = tc->inputVec->at(oldValue).second;
        MapContext mapContext = {(&tc->arrayOfInterVec->at(tc->threadId))};
        tc->client->map(k1, v1, &mapContext);
        oldValue = (*(tc->atomicIndex))++;
    }

    //sort
    auto tempVec = &(tc->arrayOfInterVec->at(tc->threadId));//[tc->threadId];
    std::sort(tempVec->begin(), tempVec->end(), comperator);
    tc->barrier->barrier();

    //shuffle
    if(tc->threadId == FIRSTTHREAD) {
        // initial K2
        K2* max = nullptr;
        bool isEmpty = check_empty_find_max(tc->arrayOfInterVec, tc->MT, &max);
        while(not isEmpty){
            IntermediateVec sameKey = {};
            for (unsigned int i = FIRSTTHREAD; i < tc->MT; ++i) {
                // in case the vector isn't empty
                if (not(tc->arrayOfInterVec->at(i).empty())) {
                    while(is_eq(max, (tc->arrayOfInterVec->at(i).back()))) {
                        (sameKey).emplace_back(((tc->arrayOfInterVec->at(i))).back());
                        ((tc->arrayOfInterVec->at(i))).pop_back();
                        //check emptyness again
                        if (((tc->arrayOfInterVec->at(i))).empty()){
                            break;
                        }
                    }
                 }
            }
            max = nullptr;
            isEmpty = check_empty_find_max(tc->arrayOfInterVec, tc->MT, &max);
            //add to queue using semaphore
            if (sem_wait(tc->mutexQueue) != 0){
                printErr("sem_wait err",tc);
            }
            tc->Queue->push_back(sameKey);
            if (sem_post(tc->mutexQueue) != 0){
                printErr("sem_post err",tc);
            }
            if (sem_post(tc->fillCount) != 0){
                printErr("sem_post err",tc);
            }
        }
        //wakeup all the threads who went down
        for (unsigned int i = FIRSTTHREAD; i < tc->MT; ++i) {
            if (sem_post(tc->fillCount) != 0){
                printErr("sem_post err",tc);
            }
        }
    }// end of shuffle

    //reduce
    while(true){
        if (sem_wait(tc->fillCount) != 0){
            printErr("sem_wait err",tc);
        }
        auto index = (*(tc->reduceAtomic))++ ;
        if ((unsigned int)index < tc->Queue->size()) {
            if (sem_wait(tc->mutexQueue) != 0){
                printErr("sem_wait err",tc);
            }
            auto pairs = &(tc->Queue->at(index));
            ReduceContext reduceContext = {tc->outputVec, tc->outAtomicIndex};
            tc->client->reduce(pairs, &reduceContext);
            if (sem_post(tc->mutexQueue) != 0){
                printErr("sem_post err",tc);
            }
        } else{
            break;
        }
    }
    return nullptr;
}


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){
    if(multiThreadLevel < 1){
        printErr("multiThreadLevel isn't legal", nullptr);
    }
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomicIndex(0);
    std::atomic<int> outAtomicIndex(0);
    std::atomic<int> reducetAtomic(0);
    std::vector<IntermediateVec> arrayOfInterVec = {};
    std::vector <IntermediateVec> Queue;
    sem_t mutexQueue;
    sem_t  fillCount;
    if (sem_init(&mutexQueue, 0, 1) != 0){
        printErr("sem_init failed\n", nullptr);
    }
    if (sem_init(&fillCount, 0, 0) != 0){
        printErr("sem_init failed\n", nullptr);
    }
    for (unsigned int i = FIRSTTHREAD; i < (unsigned int)multiThreadLevel; ++i) {
        arrayOfInterVec.push_back({});
    }
    for (unsigned int i = FIRSTTHREAD; i < (unsigned int)multiThreadLevel; ++i) {
        contexts[i] = {i, (unsigned int)multiThreadLevel, &barrier, &atomicIndex, &outAtomicIndex, &reducetAtomic,
                       &inputVec, &outputVec, &arrayOfInterVec, &client,
                       &Queue, &mutexQueue, &fillCount};
    }
    for (unsigned int i = FIRSTTHREAD+1; i < (unsigned int)multiThreadLevel; ++i) {
        if (pthread_create(threads + i, NULL, threadLogic, contexts + i) != 0){
            printErr("pthread_create failed\n",contexts + i);
        }
    }
    threadLogic(contexts);
    for (int i = FIRSTTHREAD+1; i < multiThreadLevel; ++i) {
        if (pthread_join(threads[i], NULL) != 0){
            printErr("pthread_join failed\n",contexts + i);
        }
    }
    safeExit(contexts, false);

}