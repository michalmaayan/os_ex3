//
// Created by michal.maayan on 5/30/18.
//

#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <vector>
#include <pthread.h>
#include <iostream>
#include <algorithm>    // std::sort
#include <semaphore.h>

typedef struct ThreadContext{
    int threadId;
    int MT;
    Barrier * barrier;
    std::atomic<int>* atomicIndex;
    const InputVec* inputVec;
    OutputVec* outputVec;
    IntermediateVec **arrayOfInterVec;
    const MapReduceClient* client;
    std::vector <IntermediateVec*> *Queue;
    std::atomic<bool>* flag;
    sem_t *mutexQueue;
    sem_t *fillCount;
}ThreadContext;

typedef struct MapContext{
    IntermediateVec *interVector;
    
}MapContext;

void emit2 (K2* key, V2* value, void* context){
    auto* tc = (MapContext*) context;
    tc->interVector->emplace_back(key, value);
}
void emit3 (K3* key, V3* value, void* context){

}

bool comperator(IntermediatePair &p1, IntermediatePair &p2){
    return (*p1.first < *p2.first);
}

bool check_empty_find_max(IntermediateVec **arr, int MT, K2 **max){
    bool isEmpty = true;
    for (int i = 0; i < MT; ++i) {
        if (not(*(arr[i])).empty()) {
            isEmpty = false;
            if(*max == nullptr){
                *max = (*(arr[i])).back().first;
            }
            else{
                std::cout<<"checkempty1\n";
                if(*max < (*(arr[i])).back().first)
                {
                    *max = (*(arr[i])).back().first;
                }
            }
        }
    }
    return isEmpty;
}

bool is_eq(K2 *max, IntermediatePair &p){
    std::cout<<"checkeq2\n";
    if (p.first == nullptr){
        std::cout<<"checkeq3\n";
    }


    if (*p.first < *p.first){

    }
    if(not(*max < *p.first)){
        std::cout<<"checkeq4\n";
        if(not(*p.first < *max)){
            return true;
        }
    }
    return false;
}

void* threadLogic (void* context){
    auto* tc = (ThreadContext*) context;
    int oldValue = (*(tc->atomicIndex))++ ;

    //map logic
    while(oldValue < tc->inputVec->size()) {
        auto k1 = tc->inputVec->at(oldValue).first;
        auto v1 = tc->inputVec->at(oldValue).second;
        MapContext mapContext = {(tc->arrayOfInterVec)[tc->threadId]};
        tc->client->map(k1, v1, &mapContext);
        oldValue = (*(tc->atomicIndex))++;
    }

    //sort
    auto tempVec = (tc->arrayOfInterVec)[tc->threadId];
    std::sort (tempVec->begin(), tempVec->end(), comperator);
    tc->barrier->barrier();

    //shuffle
    if(tc->threadId == 0) {
        // initial K2
        K2* max = nullptr;
        bool isEmpty = check_empty_find_max(tc->arrayOfInterVec, tc->MT, &max);
        while(not isEmpty){
            IntermediateVec *sameKey = new IntermediateVec;
            for (int i = 0; i < tc->MT; ++i) {
                // in case the vector isn't empty
                if (not(*(tc->arrayOfInterVec[i])).empty()) {
                    std::cout<<(*(tc->arrayOfInterVec[i])).empty()<<"\n";
                    std::cout<<"threadID: "<<tc->threadId<<"\n";
                    while(is_eq(max, (*(tc->arrayOfInterVec[i])).back())) {
                        (*sameKey).emplace_back((*(tc->arrayOfInterVec[i])).back());
                        (*(tc->arrayOfInterVec[i])).pop_back();
                        if ((*(tc->arrayOfInterVec[i])).empty()){
                            break;
                        }
                    }
                 }
            }
            max = nullptr;
            isEmpty = check_empty_find_max(tc->arrayOfInterVec, tc->MT, &max);

            //add to queue using semaphore
            sem_wait(tc->mutexQueue);
            tc->Queue->push_back(sameKey);
            sem_post(tc->mutexQueue);
            sem_post(tc->fillCount);
        }
        for (int i = 0; i < tc->MT; ++i) {
            sem_post(tc->fillCount);
        }
        *(tc->flag) = false;

    }

    //reduce


    return 0;
}


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomicIndex(0);
    std::atomic<bool> flag(true);
    IntermediateVec* arrayOfInterVec[multiThreadLevel];
    std::vector <IntermediateVec*> Queue;
    sem_t mutexQueue;
    sem_t  fillCount;
    sem_init(&mutexQueue, 0, 1);
    sem_init(&fillCount, 0, 0);

    for (int i = 0; i < multiThreadLevel; ++i) {
        arrayOfInterVec[i] = new IntermediateVec;
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, multiThreadLevel, &barrier, &atomicIndex, &inputVec, &outputVec, arrayOfInterVec, &client,
                       &Queue, &flag, &mutexQueue, &fillCount};
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, NULL, threadLogic, contexts + i);
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }

}