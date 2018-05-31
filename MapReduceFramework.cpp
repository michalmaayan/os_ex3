//
// Created by michal.maayan on 5/30/18.
//

#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <vector>
#include <pthread.h>


struct ThreadContext{
    int threadId;
    int MT;
    Barrier * barrier;
    std::atomic<int>* atomicIndex;
    InputVec inputVec;
    OutputVec outputVec;
    IntermediateVec vecOfVec;
    MapReduceClient* client;

    // , inVector, outVector, intermidiateVectorVector, queue, semaphore
};

struct MapContext{
    
};

void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}

void* threadLogic (void* context){
    ThreadContext* tc = (ThreadContext*) context;
    int oldValue;
    while(++*(tc->atomicIndex) < tc->MT) {
        oldValue = *(tc->atomicIndex) - 1;
        map();

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
    for (int i = 0; i < multiThreadLevel; ++i) {
        //todo add initial of vec
        contexts[i] = {i, multiThreadLevel, &barrier, &atomicIndex, &inputVec, &outputVec};

    }


}