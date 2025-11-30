#ifndef MERGER_H
#define MERGER_H

#include "RunFile.h"
#include "InputBuffer.h"
#include "OutputBuffer.h"
#include <vector>
#include <queue> // 使用 std::priority_queue
#include <iostream>

// (这些缓冲区大小定义保持不变)
#define MERGE_INPUT_BUFFER_ELEMENTS 1024
#define MERGE_OUTPUT_BUFFER_ELEMENTS 1024

template <typename T>
class Merger {
private:

    //将两个在磁盘上的 Runs 合并成一个新的 Run
    RunMetadata MergeInMem(RunFile& runFile, const RunMetadata& runA, const RunMetadata& runB) {

        // 1. 为合并后的 Run 在目录中分配一个新条目
        int newRunId = runFile.allocateNewRun();
        if (newRunId == -1) {
            throw std::runtime_error("RunFile directory is full during merge.");
        }

        // 2. 获取新 Run 的物理写入位置（文件末尾）
        long long startOffset = runFile.getAppendOffset();

        // 3. 创建输入和输出缓冲区
        InputBuffer<T> inBufA(runFile.getStream(), runA, MERGE_INPUT_BUFFER_ELEMENTS);
        InputBuffer<T> inBufB(runFile.getStream(), runB, MERGE_INPUT_BUFFER_ELEMENTS);
        OutputBuffer<T> outBuf(runFile.getStream(), startOffset, MERGE_OUTPUT_BUFFER_ELEMENTS);

        // 4. 预先加载第一个元素
        T itemA, itemB;
        bool hasA = inBufA.getNextItem(itemA);
        bool hasB = inBufB.getNextItem(itemB);

        // 5. K路归并（K=2）
        while (hasA && hasB) {
            if (itemA < itemB) {
                outBuf.setNextItem(itemA);
                hasA = inBufA.getNextItem(itemA);
            }
            else {
                outBuf.setNextItem(itemB);
                hasB = inBufB.getNextItem(itemB);
            }
        }

        // 6. 收尾：处理剩余的元素
        while (hasA) {
            outBuf.setNextItem(itemA);
            hasA = inBufA.getNextItem(itemA);
        }
        while (hasB) {
            outBuf.setNextItem(itemB);
            hasB = inBufB.getNextItem(itemB);
        }

        // 7. 刷入输出缓冲区
        outBuf.flush();
        long long totalElements = outBuf.getElementCount();

        // 8. 更新 RunFile 目录中的元数据
        runFile.updateRunMetadata(newRunId, startOffset, totalElements);

        // 9. 返回新 Run 的元数据
        return runFile.getRunMetadata(newRunId);
    }

    // 默认priority_queue是最大堆，重载实现最小堆
    struct CompareRunMetadata {
        bool operator()(const RunMetadata& a, const RunMetadata& b) {
            // 使用 > 实现最小堆
            return a.elementCount > b.elementCount;
        }
    };


public:
    // 执行最佳归并树的外排序
    RunMetadata externalMergeSort(std::vector<RunMetadata>& initialRuns, RunFile& runFile) {

        // 1. 初始化最小堆 (Priority Queue)
        std::priority_queue<RunMetadata, std::vector<RunMetadata>, CompareRunMetadata> mergeHeap;

        // 2. 将所有初始归并段放入最小堆
        for (const auto& run : initialRuns) {
            mergeHeap.push(run);
        }

        // 3. 循环，直到堆中只剩下一个 Run
        while (mergeHeap.size() > 1) {

            // 3a. 取出两个最小的 Runs
            RunMetadata runA = mergeHeap.top();
            mergeHeap.pop();
            RunMetadata runB = mergeHeap.top();
            mergeHeap.pop();

            // 3b. 合并它们
            std::cout << "Merging (Optimal) " << runA.elementCount << " elements and "
                << runB.elementCount << " elements..." << std::endl;
            RunMetadata mergedRun = MergeInMem(runFile, runA, runB);

            // 3c. 将合并后的新 Run 放回堆中
            mergeHeap.push(mergedRun);
        }

        // 4. 堆中剩下的最后一个 Run 就是最终结果
        std::cout << "Optimal external merge sort finished." << std::endl;
        return mergeHeap.top();
    }
};

#endif // MERGER_H