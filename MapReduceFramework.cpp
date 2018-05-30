//
// Created by michal.maayan on 5/30/18.
//

#include "Barrier.h"
#include "MapReduceClient.h"
#include <atomic>
#include <vector>

struct ThreadContext{
    int threadId;
    Barrier * barrier;
    std::atomic<int>* atomicIndex;
    std::vector <IntermediateVec> vecOfVec;
    
    // , inVector, outVector, intermidiateVectorVector, queue, semaphore
}

void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){

}