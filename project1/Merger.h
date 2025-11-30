#ifndef MERGER_H
#define MERGER_H

#include "RunFile.h"
#include "InputBuffer.h"
#include "OutputBuffer.h"
#include <vector>
#include <queue> // 用于管理合并队列

// 定义缓冲区大小
#define MERGE_INPUT_BUFFER_ELEMENTS 1024
#define MERGE_OUTPUT_BUFFER_ELEMENTS 1024

template <typename T>
class Merger {
private:

    // 将两个在磁盘上的有序 Runs 合并成一个新的 Run
    RunMetadata MergeInMem(RunFile& runFile, const RunMetadata& runA, const RunMetadata& runB) {

        // 为合并后的 Run 在目录中分配一个新条目
        int newRunId = runFile.allocateNewRun();
        if (newRunId == -1) {
            throw std::runtime_error("RunFile directory is full during merge.");
        }

        // 获取新 Run 的物理写入位置
        long long startOffset = runFile.getAppendOffset();

        // 创建输入和输出缓冲区
        InputBuffer<T> inBufA(runFile.getStream(), runA, MERGE_INPUT_BUFFER_ELEMENTS);
        InputBuffer<T> inBufB(runFile.getStream(), runB, MERGE_INPUT_BUFFER_ELEMENTS);
        OutputBuffer<T> outBuf(runFile.getStream(), startOffset, MERGE_OUTPUT_BUFFER_ELEMENTS);

        // 预先加载第一个元素
        T itemA, itemB;
        bool hasA = inBufA.getNextItem(itemA);
        bool hasB = inBufB.getNextItem(itemB);

        // 2路归并
        while (hasA && hasB) {
            // 比较 A 和 B 的当前元素
            if (itemA < itemB) {
                // A 较小，写入，并从 A 加载下一个
                outBuf.setNextItem(itemA);
                hasA = inBufA.getNextItem(itemA);
            }
            else {
                // 否则把B写入，并从 B 加载下一个
                outBuf.setNextItem(itemB);
                hasB = inBufB.getNextItem(itemB);
            }
        }

        // 处理剩余的元素
        while (hasA) {
            outBuf.setNextItem(itemA);
            hasA = inBufA.getNextItem(itemA);
        }
        while (hasB) {
            outBuf.setNextItem(itemB);
            hasB = inBufB.getNextItem(itemB);
        }

        // 刷入输出缓冲区，并获取新 Run 的总元素数量
        outBuf.flush();
        long long totalElements = outBuf.getElementCount();

        // 更新 RunFile 目录中的元数据
        runFile.updateRunMetadata(newRunId, startOffset, totalElements);

        // 返回新 Run 的元数据
        return runFile.getRunMetadata(newRunId);
    }

public:
    // 执行完整的外排序多路归并
    RunMetadata externalMergeSort(std::vector<RunMetadata>& initialRuns, RunFile& runFile) {

        // 使用一个队列来管理 Runs ，并把所有runs都push进去
        std::queue<RunMetadata> currentPassQueue;
        for (const auto& run : initialRuns) {
            currentPassQueue.push(run);
        }

        // 循环进行归并直到只剩下一个run
        while (currentPassQueue.size() > 1) {

            // nextPassQueue 用于存储这一轮合并产生的新 Runs
            std::queue<RunMetadata> nextPassQueue;

            // 每次从队列中取出两个 Runs 进行合并
            while (currentPassQueue.size() >= 2) {
                // 取出两个 Run
                RunMetadata runA = currentPassQueue.front();
                currentPassQueue.pop();
                RunMetadata runB = currentPassQueue.front();
                currentPassQueue.pop();

                // 合并它们
                std::cout << "Merging " << runA.elementCount << " elements and "
                    << runB.elementCount << " elements..." << std::endl;
                RunMetadata mergedRun = MergeInMem(runFile, runA, runB);

                // 放入下一轮的队列
                nextPassQueue.push(mergedRun);
            }

            // 如果这一轮有奇数个 Runs，最后一个未被合并的 Run 直接进入下一轮
            if (!currentPassQueue.empty()) {
                nextPassQueue.push(currentPassQueue.front());
                currentPassQueue.pop();
            }

            // 下一轮开始
            currentPassQueue = nextPassQueue;
        }

        // 当队列只剩一个 Run 时，排序完成
        std::cout << "External merge sort finished." << std::endl;
        return currentPassQueue.front();
    }
};

#endif // MERGER_H