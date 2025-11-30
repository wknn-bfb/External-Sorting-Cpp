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

/* 
* 情景设置
* 元素类型是int
* 内存有4MB = 1M (1024*1024) 个元素 * 4字节
* 磁盘中有10M个元素，也就是40MB文件
* IO缓冲区为4KB，也就是1024个元素
*/

typedef int T;
const int ELEMENTS_PER_RUN_IN_MEM = 1024 * 1024;
const long long TOTAL_ELEMENTS_TO_SORT = 10 * 1024 * 1024;
const int IO_BUFFER_SIZE_ELEMENTS = 1024;

const std::string ORIGINAL_DATA_FILE = "original_data.dat"; // 原始数据文件
const std::string RUN_STORAGE_FILE = "runs.dat";  // 存储归并段的文件

// 辅助函数：生成原始数据文件
void createOriginalDataFile(){
    std::cout << "Creating original data file (" << ORIGINAL_DATA_FILE << ") with "
        << TOTAL_ELEMENTS_TO_SORT << " elements..." << std::endl;

    // 打开原始数据文件
    std::ofstream outFile(ORIGINAL_DATA_FILE, std::ios::binary);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to create original data file.");
    }

    // 随机生成整数并循环填充
    srand((unsigned int)time(NULL));
    T data;
    for (long long i = 0; i < TOTAL_ELEMENTS_TO_SORT; ++i) {
        data = rand() % INT_MAX; // 生成 0 到 INT_MAX 之间的随机整数
        outFile.write(reinterpret_cast<const char*>(&data), sizeof(T));
    }
    outFile.close();
}

// 辅助函数：验证最终的 Run 是否正确排序
bool verifySortedRun(RunFile& runFile, const RunMetadata& finalRun) {
    std::cout << "Verifying final run..." << std::endl;

    InputBuffer<T> inBuf(runFile.getStream(), finalRun, IO_BUFFER_SIZE_ELEMENTS);

    T lastItem;
    T currentItem;

    // 读取第一个
    if (!inBuf.getNextItem(lastItem)) {
        std::cout << "Verification complete (file was empty)." << std::endl;
        return true; // 空文件认为有序
    }

    // 循环比较，上一个值应该<=当前值
    while (inBuf.getNextItem(currentItem)) {
        if (currentItem < lastItem) {
            // 顺序颠倒
            std::cerr << "Verification FAILED: " << currentItem << " < " << lastItem << std::endl;
            return false;
        }
        lastItem = currentItem;
    }

    std::cout << "Verification SUCCESS: Final run is sorted." << std::endl;
    return true;
}


int main() {
    try {
        // 创建初始数据
        createOriginalDataFile();

        // 初始化RunFile
        RunFile runFile(RUN_STORAGE_FILE);
        runFile.create(20); // 理论上正好10个runs，初始化可以大一些
        if (!runFile.open()) {
            throw std::runtime_error("Failed to open run file.");
        }

        // 阶段1：生成初始归并段
        std::cout << "\n--- Phase 1: Generating Initial Runs ---" << std::endl;
        RunGenerator<T> generator(ELEMENTS_PER_RUN_IN_MEM); // 初始化归并段生成器

        // 调用内部sort生成10个有序归并段
        auto start_gen = std::chrono::high_resolution_clock::now();
        std::vector<RunMetadata> initialRuns = generator.generateRuns(ORIGINAL_DATA_FILE, runFile);
        auto end_gen = std::chrono::high_resolution_clock::now();

        // 记录生成时间
        std::cout << "Run generation finished in "
            << std::chrono::duration<double>(end_gen - start_gen).count() << "s." << std::endl;
        std::cout << "Generated " << initialRuns.size() << " initial runs." << std::endl;

        // 阶段2: 归并合并
        std::cout << "\n--- Phase 2: Merging Runs ---" << std::endl;
        Merger<T> merger; // 初始化归并器

        // 用外部归并排序合成一个大的有序段并记录时间
        auto start_merge = std::chrono::high_resolution_clock::now();
        RunMetadata finalRun = merger.externalMergeSort(initialRuns, runFile);
        auto end_merge = std::chrono::high_resolution_clock::now();

        std::cout << "Merge finished in "
            << std::chrono::duration<double>(end_merge - start_merge).count() << "s." << std::endl;

        // 阶段3：验证结果
        std::cout << "\n--- Phase 3: Verification ---" << std::endl;
        verifySortedRun(runFile, finalRun);

        // 关闭runFile
        runFile.close();

    }
    catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}