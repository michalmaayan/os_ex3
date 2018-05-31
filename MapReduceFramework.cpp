//
// Created by michal.maayan on 5/30/18.
//

#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <vector>
#include <pthread.h>


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
}
void emit3 (K3* key, V3* value, void* context){

}

void* threadLogic (void* context){
    auto* tc = (ThreadContext*) context;
    int oldValue;
    while(++*(tc->atomicIndex) < tc->MT) {
        oldValue = *(tc->atomicIndex) - 1;
        auto k1 = tc->inputVec->at(oldValue).first;
        auto v1 = tc->inputVec->at(oldValue).second;
        MapContext mapContext = {(tc->arrayOfInterVec)[oldValue]};
        tc->client->map(k1, v1, &mapContext);

    }
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
        //todo add initial of vec
        contexts[i] = {i, multiThreadLevel, &barrier, &atomicIndex, &inputVec, &outputVec, arrayOfInterVec, &client};
    }


}