#ifndef RUN_FILE_H
#define RUN_FILE_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <stdexcept>

// 归并段元数据
struct RunMetadata {
    long long startOffset;   // 该归并段在文件中的起始字节偏移
    long long elementCount;  // 该归并段包含的元素数量
    bool isUsed;             // 该归并段是否正在被使用

    // 默认构造函数
    RunMetadata() : startOffset(0), elementCount(0), isUsed(false) {}
};

// 归并段文件头
struct RunFileHeader {
    char magic[4];       // 文件标识，例如 "RUNS"
    int maxRuns;         // 目录区能存储的最大 Run 数量
    int currentRunCount; // 文件中当前活跃的 Run 数量

    RunFileHeader(int max_runs = 0)
        : maxRuns(max_runs), currentRunCount(0) {
        memcpy(magic, "RUNS", 4);
    }
};

// 归并文件类
class RunFile {
private:
    std::fstream file;      // 文件流对象
    std::string filename;
    RunFileHeader header;
    std::vector<RunMetadata> directory; // 目录区的内存副本

    // 将内存中的单个元数据条目写回磁盘
    void writeMetadataToDisk(int runId) {
        if (!file.is_open() || runId < 0 || runId >= header.maxRuns) {
            return;
        }
        // 定位到该元数据在文件中的位置
        long long diskOffset = sizeof(RunFileHeader) + (long long)runId * sizeof(RunMetadata);
        file.seekp(diskOffset);
        // 写入
        file.write(reinterpret_cast<const char*>(&directory[runId]), sizeof(RunMetadata));
    }

public:
    RunFile(const std::string& fname) : filename(fname) {}

    ~RunFile() {
        close();
    }

    // 创建并初始化 Run 文件
    bool create(int maxRuns = 1000) {
        header = RunFileHeader(maxRuns);

        file.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!file.is_open()) return false;

        // 写入文件头
        file.write(reinterpret_cast<const char*>(&header), sizeof(RunFileHeader));

        // 写入空的目录区
        directory.assign(maxRuns, RunMetadata()); // 创建空的元数据
        for (int i = 0; i < maxRuns; ++i) {
            file.write(reinterpret_cast<const char*>(&directory[i]), sizeof(RunMetadata));
        }

        file.close();
        return true;
    }

    // 打开一个已存在的 Run 文件
    bool open() {
        // 以读写模式打开
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) return false;

        // 读取文件头
        file.read(reinterpret_cast<char*>(&header), sizeof(RunFileHeader));
        if (std::string(header.magic, 4) != "RUNS") {
            file.close();
            return false; // 不是有效的 Run 文件
        }

        // 读取整个目录区到内存
        directory.resize(header.maxRuns);
        for (int i = 0; i < header.maxRuns; ++i) {
            file.read(reinterpret_cast<char*>(&directory[i]), sizeof(RunMetadata));
        }

        // 保持文件打开
        return true;
    }

    // 关闭文件
    void close() {
        if (file.is_open()) {
            file.flush();
            file.close();
        }
    }

    // 在目录区分配一个新的 Run 条目
    int allocateNewRun() {
        for (int i = 0; i < header.maxRuns; ++i) {
            if (!directory[i].isUsed) {
                directory[i].isUsed = true;
                directory[i].startOffset = 0;
                directory[i].elementCount = 0;

                // 将这个新条目写回磁盘
                writeMetadataToDisk(i);

                header.currentRunCount++;
                return i; // 返回 Run ID
            }
        }
        return -1; // 目录已满
    }

    // 更新一个 Run 的元数据
    void updateRunMetadata(int runId, long long startOffset, long long elementCount) {
        if (runId < 0 || runId >= header.maxRuns) {
            throw std::out_of_range("Invalid runId in updateRunMetadata.");
        }
        directory[runId].startOffset = startOffset;
        directory[runId].elementCount = elementCount;

        // 将更新写回磁盘
        writeMetadataToDisk(runId);
    }

    // 获取指定 Run 的元数据
    RunMetadata getRunMetadata(int runId) {
        if (runId < 0 || runId >= header.maxRuns) {
            throw std::out_of_range("Invalid runId in getRunMetadata.");
        }
        return directory[runId];
    }

    // 获取数据区的起始/追加偏移
    long long getAppendOffset() {
        // 定位到文件末尾
        file.seekp(0, std::ios::end);
        return file.tellp();
    }

    // 暴露文件流
    std::fstream& getStream() {
        return file;
    }
};

#endif // RUN_FILE_H