#ifndef RUN_GENERATOR_H
#define RUN_GENERATOR_H

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <fstream>
#include <string>
#include <iostream>
#include <limits>

#include "RunFile.h"
#include "LoserTree.h"

#ifndef RG_BUFFER_SIZE
#define RG_BUFFER_SIZE (1024 * 1024)
#endif

template <typename T>
class RunGenerator {
public:
    // 构造函数：初始化资源和线程同步状态
    RunGenerator(int memSizeForLoserTree, int bufferSize = RG_BUFFER_SIZE)
        : K(memSizeForLoserTree),
        bufSize(bufferSize),
        // 新版 LoserTree 构造函数只接受大小，内部处理哨兵
        loserTree(memSizeForLoserTree),
        stop_threads(false),
        standby_input_ready(false),
        standby_output_busy(false),
        input_eof(false)
    {
        inBufA.resize(bufSize); inBufB.resize(bufSize);
        outBufA.resize(bufSize); outBufB.resize(bufSize);

        activeIn = &inBufA; standbyIn = &inBufB;
        activeOut = &outBufA; standbyOut = &outBufB;

        activeIn->resize(0); // 初始为空，触发第一次读取
        activeOut->clear();
    }

    ~RunGenerator() {
        stop_threads = true;
        cv_input.notify_all();
        cv_output.notify_all();
        if (inputThread.joinable()) inputThread.join();
        if (outputThread.joinable()) outputThread.join();
    }

    std::vector<RunMetadata> generateRuns(const std::string& inputFilename, RunFile& runFile) {
        runFilePtr = &runFile;
        inputFile.open(inputFilename, std::ios::binary);
        if (!inputFile) throw std::runtime_error("Cannot open input file");

        currentRunId = runFile.allocateNewRun();
        currentRunStartOffset = runFile.getAppendOffset();
        totalElementsInRun = 0;
        generatedRuns.clear();

        inputThread = std::thread(&RunGenerator::inputWorker, this);
        outputThread = std::thread(&RunGenerator::outputWorker, this);

        computeWorker();

        inputFile.close();
        return generatedRuns;
    }

private:
    const int K;
    const int bufSize;
    LoserTree<T> loserTree;

    // Buffers
    std::vector<T> inBufA, inBufB;
    std::vector<T> outBufA, outBufB;
    std::vector<T>* activeIn;
    std::vector<T>* standbyIn;
    std::vector<T>* activeOut;
    std::vector<T>* standbyOut;

    int activeInIdx = 0;
    // activeOutIdx 实际上不再需要作为成员变量维护，因为我们用 push_back，但在 logic 中保留用于兼容

    std::mutex mtx;
    std::condition_variable cv_input, cv_output, cv_compute;
    bool standby_input_ready;
    bool standby_output_busy;
    bool input_eof;
    std::atomic<bool> stop_threads;

    std::thread inputThread;
    std::thread outputThread;

    std::ifstream inputFile;
    RunFile* runFilePtr;
    long long currentRunStartOffset;
    long long totalElementsInRun;
    int currentRunId;
    std::vector<RunMetadata> generatedRuns;

    // --- 辅助函数：获取下一个输入元素 ---
    //      - 从 activeIn 读取
    //      - activeIn 耗尽时自动与 standbyIn 交换
    //      - 等待 inputWorker 填充 standbyIn
    //      - 判断 EOF
    //      - 全流程无 busy-wait
    bool pullNextInput(T& val, std::unique_lock<std::mutex>& lock) {

        // 使用循环结构确保交换缓冲区后继续尝试读取
        while (true) {

            // --- ① 快速路径：activeIn 还有可读元素 ---
            // activeInIdx 是当前读指针，如果还没到 size，说明 buffer 里有数据
            if (activeInIdx < activeIn->size()) {
                val = (*activeIn)[activeInIdx++]; // 取出一个元素
                return true;                     // 成功
            }

            // --- ② active buffer 已空，检查 standby buffer 是否已经 ready ---
            // 如果 inputWorker 已经填好了 standbyIn，则现在可以交换缓冲区
            if (standby_input_ready) {
                std::swap(activeIn, standbyIn); // 交换 active / standby（双缓冲关键点）
                activeInIdx = 0;                // 重置读指针
                standby_input_ready = false;    // 标记 standby 已经被消费
                cv_input.notify_one();          // 唤醒 inputWorker 去读下一块 data
                continue;                       // 交换后继续从 activeIn 尝试读取
            }

            // --- ③ standby 没准备好，但 active 又已空 → 是否到 EOF？ ---
            // 如果 inputWorker 已经读到 EOF，则不再产生任何数据
            if (input_eof) return false;

            // --- ④ standby 不 ready 且没有 EOF → 需要等待 inputWorker 填充 ---
            // Compute 线程在这里睡眠，直到：
            //   - standby_input_ready = true（inputWorker 读好一块）
            //   - 或者 input_eof = true（文件读完）
            //   - 或 stop_threads = true（系统退出）
            cv_compute.wait(lock, [this] {
                return standby_input_ready || input_eof || stop_threads;
            });

            // --- ⑤ 若收到停止信号 ---
            if (stop_threads) return false;

            // 循环继续，进入下一轮判断（要么 swap，要么 EOF，要么快速路径）
        }
    }

    // --- Input Worker ---
    void inputWorker() {
        std::unique_lock<std::mutex> lock(mtx);
        while (!stop_threads) {
            cv_input.wait(lock, [this] { return !standby_input_ready || stop_threads; });
            if (stop_threads) break;

            standbyIn->resize(bufSize);
            lock.unlock();
            inputFile.read(reinterpret_cast<char*>(standbyIn->data()),
                (long long)standbyIn->size() * sizeof(T));
            int count = (int)(inputFile.gcount() / sizeof(T));
            lock.lock();

            standbyIn->resize(count);
            if (inputFile.eof() || count == 0) input_eof = true;
            standby_input_ready = true;
            cv_compute.notify_one();
        }
    }

    // --- Output Worker ---
    void outputWorker() {
        std::unique_lock<std::mutex> lock(mtx);
        while (!stop_threads) {
            cv_output.wait(lock, [this] { return standby_output_busy || stop_threads; });
            if (stop_threads) break;

            int count = (int)standbyOut->size();
            lock.unlock();
            if (count > 0) {
                runFilePtr->getStream().seekp(currentRunStartOffset + totalElementsInRun * sizeof(T));
                runFilePtr->getStream().write(reinterpret_cast<char*>(standbyOut->data()),
                    (long long)count * sizeof(T));
            }
            lock.lock();

            if (count > 0) totalElementsInRun += count;
            standby_output_busy = false;
            cv_compute.notify_one();
        }
    }

    // --- Compute Worker (使用 RunID 逻辑) ---
    void computeWorker() {
        std::unique_lock<std::mutex> lock(mtx);

        // 1. 初始填充
        std::vector<T> initialData;
        initialData.reserve(K);
        T val;
        while (initialData.size() < K && pullNextInput(val, lock)) {
            initialData.push_back(val);
        }

        // 初始化树，默认 RunID = 1
        loserTree.initialize(initialData);

        int currentTreeRunID = 1; // 当前正在生成的 Run ID

        // 2. 主循环
        while (true) {
            // A. 获取赢家
            RunNode<T> winnerNode = loserTree.getWinner();

            // B. 全局结束检查 (赢家是哨兵 -> 树空了)
            if (winnerNode.runID == std::numeric_limits<int>::max()) {
                break;
            }

            // C. 当前Run结束（winner 属于未来的 run），说明输入耗尽或者元素都被冻结
            if (winnerNode.runID > currentTreeRunID) {
                // 1. 刷出 Output
                if (!activeOut->empty()) {
                    if (standby_output_busy)
                        cv_compute.wait(lock, [this] { return !standby_output_busy || stop_threads; });
                    if (stop_threads) break;

                    std::swap(activeOut, standbyOut);
                    standby_output_busy = true;
                    cv_output.notify_one();
                    activeOut->clear();
                }

                // 2. 等待 Output 写完
                if (standby_output_busy)
                    cv_compute.wait(lock, [this] { return !standby_output_busy || stop_threads; });
                if (stop_threads) break;

                // 3. 记录 Run
                if (totalElementsInRun > 0) {
                    runFilePtr->updateRunMetadata(currentRunId, currentRunStartOffset, totalElementsInRun);
                    generatedRuns.push_back(runFilePtr->getRunMetadata(currentRunId));
                }

                // 4. 开启新 Run
                currentRunId = runFilePtr->allocateNewRun();
                currentRunStartOffset = runFilePtr->getAppendOffset();
                totalElementsInRun = 0;

                // 5. 更新当前追踪的 RunID
                currentTreeRunID = winnerNode.runID;
            }

            // D. 输出赢家
            activeOut->push_back(winnerNode.value);

            // Output 满：swap 到 standbyOut，outputWorker 写盘
            if (activeOut->size() >= bufSize) {
                if (standby_output_busy)
                    cv_compute.wait(lock, [this] { return !standby_output_busy || stop_threads; });
                if (stop_threads) break;

                std::swap(activeOut, standbyOut);
                standby_output_busy = true;
                cv_output.notify_one();
                activeOut->clear();
            }

            // E. 读取新值并替换
            if (!pullNextInput(val, lock)) {
                // 拉不到下一个值，则该节点被设为哨兵
                loserTree.setWinnerToSentinel();
            }
            else {
                //拉到下一个值，比较大小判断属于当前run还是下一个run
                int newRunID = (val < winnerNode.value) ? currentTreeRunID + 1 : currentTreeRunID;
                loserTree.replaceWinner(val, newRunID);
            }
        }

        // --- 收尾 ---
        if (standby_output_busy) cv_compute.wait(lock, [this] { return !standby_output_busy || stop_threads; });
        
        //刷 output
        if (!activeOut->empty()) {
            std::swap(activeOut, standbyOut);
            standby_output_busy = true;
            cv_output.notify_one();
            cv_compute.wait(lock, [this] { return !standby_output_busy || stop_threads; });
        }
        
        //写 metadata
        if (totalElementsInRun > 0) {
            runFilePtr->updateRunMetadata(currentRunId, currentRunStartOffset, totalElementsInRun);
            generatedRuns.push_back(runFilePtr->getRunMetadata(currentRunId));
        }

        //唤醒 input / output 线程退出
        stop_threads = true;
        cv_input.notify_all();
        cv_output.notify_all();
    }
};

#endif