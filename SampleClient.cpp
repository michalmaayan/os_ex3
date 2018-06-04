#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>

class VString : public V1 {
public:
	VString(std::string content) : content(content) { }
	std::string content;
};

class KChar : public K2, public K3{
public:
	KChar(char c) : c(c) { }
	virtual bool operator<(const K2 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	virtual bool operator<(const K3 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	char c;
};

class VCount : public V2, public V3{
public:
	VCount(int count) : count(count) { }
	int count;
};


class CounterClient : public MapReduceClient {
public:
	void map(const K1* key, const V1* value, void* context) const {
		std::array<int, 256> counts;
		counts.fill(0);
		for(const char& c : static_cast<const VString*>(value)->content) {
			counts[(unsigned char) c]++;
		}

		for (int i = 0; i < 256; ++i) {
			if (counts[i] == 0)
				continue;

			KChar* k2 = new KChar(i);
			VCount* v2 = new VCount(counts[i]);
			emit2(k2, v2, context);
		}
	}

	virtual void reduce(const IntermediateVec* pairs, 
		void* context) const {
		const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
		int count = 0;
		for(const IntermediatePair& pair: *pairs) {
			count += static_cast<const VCount*>(pair.second)->count;
			delete pair.first;
			delete pair.second;
		}
		KChar* k3 = new KChar(c);
		VCount* v3 = new VCount(count);
		emit3(k3, v3, context);
	}
};

void printIntermediatePair(IntermediatePair * pair){
    char c = ((const KChar*)(*pair).first)->c;
    int count = ((const VCount*)(*pair).second)->count;
    printf("key: %c, value: %d\n",c, count);

}
void printInterKey(K2* key){
    char k = ((const KChar*)key)->c;
    printf("%c",k);
}


int main(int argc, char** argv)
{

    printf("STARTING TEST1:\n");
    CounterClient client;
	InputVec inputVec;
	OutputVec outputVec;
    int num_of_in = 300;
    VString* arr[num_of_in];
    for (int i = 0; i < num_of_in; ++i){
        arr[i] = new VString("abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz");
        inputVec.push_back({nullptr, arr[i]});
    }
//    inputVec.push_back({nullptr, &s9});
	runMapReduceFramework(client, inputVec, outputVec, 4);

	for (OutputPair& pair: outputVec) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n",
			c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}
    printf("END TEST1!!!\n");
    // TEST 2
    printf("STARTING TEST2:\n");
    CounterClient client2;
    InputVec inputVec2;
    OutputVec outputVec2;
    int num_of_in2 = 300;
    VString* arr2[num_of_in];
    for (int i = 0; i < num_of_in2; ++i){
        arr2[i] = new VString("abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz");
        inputVec2.push_back({nullptr, arr2[i]});
    }
//    inputVec.push_back({nullptr, &s9});
    runMapReduceFramework(client2, inputVec2, outputVec2, 10);

    for (OutputPair& pair: outputVec2) {
        char c = ((const KChar*)pair.first)->c;
        int count = ((const VCount*)pair.second)->count;
        printf("The character %c appeared %d time%s\n",
               c, count, count > 1 ? "s" : "");
        delete pair.first;
        delete pair.second;
    }
    printf("END TEST2!!!\n");

	// TEST 2
	printf("STARTING TEST3:\n");
	CounterClient client3;
	InputVec inputVec3;
	OutputVec outputVec3;
	int num_of_in3 = 900;
	VString* arr3[num_of_in];
	for (int i = 0; i < num_of_in3; ++i){
		arr3[i] = new VString("abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz");
		inputVec3.push_back({nullptr, arr3[i]});
	}
//    inputVec.push_back({nullptr, &s9});
	runMapReduceFramework(client3, inputVec3, outputVec3, 900);

	for (OutputPair& pair: outputVec3) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n",
			   c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}
	printf("END TEST3!!!\n");

	return 0;
}

