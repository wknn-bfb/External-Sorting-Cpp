#ifndef RUN_GENERATOR_H
#define RUN_GENERATOR_H

#include "RunFile.h"
#include "OutputBuffer.h"
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>

template <typename T>
class RunGenerator {
private:
    int elementsPerRun; // 内存中一次可以排序的元素数量
    std::vector<T> tempBuffer; // 用于内部排序的内存缓冲区

public:
    // 构造函数：根据指定的内存大小初始化内部缓冲区
    RunGenerator(int elementsInMem) : elementsPerRun(elementsInMem) {
        tempBuffer.resize(elementsInMem);
    }

    // 从原始文件读取数据，排序后写入 RunFile，并返回所有 Run 的元数据
    std::vector<RunMetadata> generateRuns(const std::string& originalFileName, RunFile& runFile) {

        std::vector<RunMetadata> generatedRuns;

        std::ifstream inputFile(originalFileName, std::ios::in | std::ios::binary);
        if (!inputFile.is_open()) {
            throw std::runtime_error("Could not open original data file.");
        }

        bool moreData = true;
        while (moreData) {
            inputFile.read(reinterpret_cast<char*>(tempBuffer.data()),
                (long long)elementsPerRun * sizeof(T));

            int elementsRead = inputFile.gcount() / sizeof(T);

            if (elementsRead == 0) {
                break;
            }
            if (elementsRead < elementsPerRun) {
                moreData = false;
                tempBuffer.resize(elementsRead);
            }

            std::sort(tempBuffer.begin(), tempBuffer.end());

            int runId = runFile.allocateNewRun();
            if (runId == -1) {
                throw std::runtime_error("RunFile directory is full.");
            }

            long long startOffset = runFile.getAppendOffset();

            int outputBlockSize = 1024;
            OutputBuffer<T> outBuf(runFile.getStream(), startOffset, outputBlockSize);

            for (int i = 0; i < elementsRead; ++i) {
                outBuf.setNextItem(tempBuffer[i]);
            }
            outBuf.flush();

            runFile.updateRunMetadata(runId, startOffset, elementsRead);

            generatedRuns.push_back(runFile.getRunMetadata(runId));

            if (moreData == false) {
                tempBuffer.resize(elementsPerRun);
            }
        }

        inputFile.close();
        return generatedRuns;
    }
};

#endif // RUN_GENERATOR_H