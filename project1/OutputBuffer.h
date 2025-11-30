#ifndef OUTPUT_BUFFER_H
#define OUTPUT_BUFFER_H

#include "RunFile.h"
#include <vector>

template <typename T>
class OutputBuffer {
private:
    std::fstream& fileStream;   // 对 RunFile 文件流的引用
    long long runStartOffset;   // 此 Run 在文件中的起始偏移
    int bufferSizeInElements;   // 缓冲区大小（以元素为单位）

    std::vector<T> buffer;      // 内存缓冲区
    int currentBufferIndex;     // 当前缓冲区中已填充的元素数量
    long long totalElementsWritten; // 已写入此 Run 的总元素数量

    // 将内存缓冲区的数据写入磁盘
    void writeBlock() {
        if (!fileStream.is_open() || currentBufferIndex == 0) {
            return; // 文件未打开或缓冲区为空
        }

        // 计算本次写入在文件中的绝对位置
        long long writeOffset = runStartOffset + (totalElementsWritten * sizeof(T));
        fileStream.seekp(writeOffset);

        // 将缓冲区内容写入文件
        fileStream.write(reinterpret_cast<const char*>(buffer.data()),
            (long long)currentBufferIndex * sizeof(T));

        // 更新统计
        totalElementsWritten += currentBufferIndex;
        currentBufferIndex = 0; // 重置缓冲区
    }

public:
    // 构造函数
    OutputBuffer(std::fstream& fs, long long startOffset, int bufferSizeInElements)
        : fileStream(fs),
        runStartOffset(startOffset),
        bufferSizeInElements(bufferSizeInElements),
        currentBufferIndex(0),
        totalElementsWritten(0)
    {
        // 预分配内存以提高效率
        buffer.resize(bufferSizeInElements);
    }

    // 析构函数
    ~OutputBuffer() {
        flush();
    }

    // 向缓冲区添加一个元素
    void setNextItem(const T& item) {
        // 将元素放入内存缓冲区
        buffer[currentBufferIndex++] = item;

        // 检查缓冲区是否已满
        if (currentBufferIndex == bufferSizeInElements) {
            writeBlock(); // 刷入磁盘
        }
    }

    // 手动刷入缓冲区
    void flush() {
        if (currentBufferIndex > 0) {
            writeBlock();
        }
        fileStream.flush(); // 确保操作系统已写入
    }

    // 获取此缓冲区总共写入了多少元素
    long long getElementCount() const {
        return totalElementsWritten + currentBufferIndex;
    }
};

#endif // OUTPUT_BUFFER_H