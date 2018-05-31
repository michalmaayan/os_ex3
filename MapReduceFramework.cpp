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

typedef struct ThreadContext{
    int threadId;
    int MT;
    Barrier * barrier;
    std::atomic<int>* atomicIndex;
    const InputVec* inputVec;
    OutputVec* outputVec;
    IntermediateVec **arrayOfInterVec;
    const MapReduceClient* client;

    // , inVector, outVector, intermidiateVectorVector, queue, semaphore
}ThreadContext;

typedef struct MapContext{
    IntermediateVec *interVector;
    
}MapContext;

void emit2 (K2* key, V2* value, void* context){
    auto* tc = (MapContext*) context;
    tc->interVector->emplace_back(key, value);
    std::cout<<"bla\n";
}
void emit3 (K3* key, V3* value, void* context){

}

bool comperator(IntermediatePair p1, IntermediatePair p2){
    return (p1.first<p2.first);
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
        std::cout<<"old value: "<<oldValue<<"\n";
    }
    //sort
    auto tempVec = (tc->arrayOfInterVec)[tc->threadId];
    std::sort (tempVec->begin(), tempVec->end(), comperator);
    tc->barrier->barrier();

    //shuffle
    if(tc->threadId == 0) {
        std::cout<<"bla";
    }

    //barrier

    return 0;
}


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomicIndex(0);
    IntermediateVec* arrayOfInterVec[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i) {
        arrayOfInterVec[i] = new IntermediateVec;
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, multiThreadLevel, &barrier, &atomicIndex, &inputVec, &outputVec, arrayOfInterVec, &client};
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, NULL, threadLogic, contexts + i);
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }

}