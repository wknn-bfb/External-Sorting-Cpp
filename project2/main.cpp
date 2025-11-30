#include "RunFile.h"
#include "RunGenerator.h"
#include "Merger.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <climits> 
#include <chrono>

// --- 定义元素类型 ---
typedef int T;

// --- 配置常量 ---
// 1. 内存限制：败者树的大小 (k) 使用 1M (1024*1024) 个整数作为内存工作区
const int K_LOSER_TREE_SIZE = 1024 * 1024;

// 2. 原始文件大小：我们要排序多少个元素 —— 10M 个元素 * 4 字节/整数 = 40MB 文件
const long long TOTAL_ELEMENTS_TO_SORT = 10 * 1024 * 1024;

// 3. 缓冲区大小：I/O 缓冲区应为多大（以元素为单位）
const int IO_BUFFER_SIZE_ELEMENTS = 1024;

// 4. 文件名
const std::string ORIGINAL_DATA_FILE = "original_data.dat";
const std::string RUN_STORAGE_FILE = "runs.dat";


// 创建一个大的、未排序的原始数据文件
void createDummyDataFile() {
    std::cout << "Creating dummy data file (" << ORIGINAL_DATA_FILE << ") with "
        << TOTAL_ELEMENTS_TO_SORT << " elements..." << std::endl;

    std::ofstream outFile(ORIGINAL_DATA_FILE, std::ios::binary);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to create dummy data file.");
    }

    srand((unsigned int)time(NULL));
    T data;
    for (long long i = 0; i < TOTAL_ELEMENTS_TO_SORT; ++i) {
        data = rand() % INT_MAX;
        outFile.write(reinterpret_cast<const char*>(&data), sizeof(T));
    }
    outFile.close();
}

// 验证最终的 Run 是否真的排好序了
bool verifySortedRun(RunFile& runFile, const RunMetadata& finalRun) {
    std::cout << "Verifying final run..." << std::endl;

    InputBuffer<T> inBuf(runFile.getStream(), finalRun, IO_BUFFER_SIZE_ELEMENTS);

    T lastItem;
    T currentItem;

    if (!inBuf.getNextItem(lastItem)) {
        std::cout << "Verification complete (file was empty)." << std::endl;
        return true;
    }

    while (inBuf.getNextItem(currentItem)) {
        if (currentItem < lastItem) {
            std::cerr << "Verification FAILED: " << currentItem << " < " << lastItem << std::endl;
            return false;
        }
        lastItem = currentItem;
    }

    std::cout << "Verification SUCCESS: Final run is sorted." << std::endl;
    return true;
}

// 主函数
int main() {
    try {
        // --- 0. 创建假数据 ---
        createDummyDataFile();

        // --- 1. 初始化 RunFile ---
        RunFile runFile(RUN_STORAGE_FILE);
        if (!runFile.create(10000)) {
            throw std::runtime_error("Failed to create run file.");
        }
        if (!runFile.open()) {
            throw std::runtime_error("Failed to open run file.");
        }

        // --- 2. 阶段 1: 生成初始归并段 (使用 Project 2 的 RunGenerator) ---
        std::cout << "\n--- Phase 1: Generating Initial Runs (Project 2: Loser Tree) ---" << std::endl;

        // 使用 K_LOSER_TREE_SIZE 初始化新的 RunGenerator
        RunGenerator<T> generator(K_LOSER_TREE_SIZE);

        auto start_gen = std::chrono::high_resolution_clock::now();

        // 调用新的 generateRuns
        std::vector<RunMetadata> initialRuns = generator.generateRuns(ORIGINAL_DATA_FILE, runFile);

        auto end_gen = std::chrono::high_resolution_clock::now();

        std::cout << "Run generation finished in "
            << std::chrono::duration<double>(end_gen - start_gen).count() << "s." << std::endl;

        // 验证：我们期望 Runs 的数量大约是 Project 1 的一半
        std::cout << "Generated " << initialRuns.size() << " initial runs (unequal length)." << std::endl;

        // 打印出每个 run 的长度，以验证它们是不等长的
        long long totalElementsGenerated = 0;
        for (const auto& run : initialRuns) {
            std::cout << "  - Run: " << run.elementCount << " elements" << std::endl;
            totalElementsGenerated += run.elementCount;
        }
        if (totalElementsGenerated != TOTAL_ELEMENTS_TO_SORT) {
            std::cerr << "Error: Generated elements count mismatch!" << std::endl;
        }


        // --- 3. 阶段 2: 归并合并 (使用 Project 2 的 Merger) ---
        std::cout << "\n--- Phase 2: Merging Runs (Project 2: Optimal Merge Tree) ---" << std::endl;

        // Merger 类包含了新的 externalMergeSort (使用最小堆)
        Merger<T> merger;

        auto start_merge = std::chrono::high_resolution_clock::now();

        // 调用新的 externalMergeSort
        RunMetadata finalRun = merger.externalMergeSort(initialRuns, runFile);

        auto end_merge = std::chrono::high_resolution_clock::now();

        std::cout << "Merge finished in "
            << std::chrono::duration<double>(end_merge - start_merge).count() << "s." << std::endl;

        // --- 4. 验证 ---
        std::cout << "\n--- Phase 3: Verification ---" << std::endl;
        verifySortedRun(runFile, finalRun);

        // --- 5. 清理 ---
        runFile.close();

    }
    catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}