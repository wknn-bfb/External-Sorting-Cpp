#ifndef INPUT_BUFFER_H
#define INPUT_BUFFER_H

#include "RunFile.h"
#include <vector>

template <typename T>
class InputBuffer {
private:
    std::fstream& fileStream;   // 对 RunFile 文件流的引用
    RunMetadata runMeta;        // 此 Run 的元数据
    int bufferSizeInElements;   // 缓冲区大小（以元素为单位）

    std::vector<T> buffer;      // 内存缓冲区
    int currentIndexInBuffer;   // 当前在缓冲区中读到的位置
    int elementsInBuffer;       // 缓冲区中当前有效的元素数量

    long long totalElementsRead;    // 已从此 Run 读取的总元素数量

    // 从磁盘读取下一个数据块到内存缓冲区
    bool readBlock() {
        // 检查是否已读完
        if (totalElementsRead >= runMeta.elementCount) {
            return false; // 已到达此 Run 的末尾
        }

        // 计算本次要读取多少元素
        long long elementsRemainingInRun = runMeta.elementCount - totalElementsRead;
        int elementsToRead = (int)std::min((long long)bufferSizeInElements, elementsRemainingInRun);

        if (elementsToRead <= 0) {
            return false;
        }

        // 计算本次读取在文件中的绝对位置
        long long readOffset = runMeta.startOffset + (totalElementsRead * sizeof(T));
        fileStream.seekg(readOffset);

        // 读取
        buffer.resize(elementsToRead); // 调整缓冲区大小
        fileStream.read(reinterpret_cast<char*>(buffer.data()),
            (long long)elementsToRead * sizeof(T));

        // 更新统计
        elementsInBuffer = elementsToRead;
        totalElementsRead += elementsToRead;
        currentIndexInBuffer = 0; // 重置缓冲区索引

        return true;
    }

public:
    // 构造函数
    InputBuffer(std::fstream& fs, const RunMetadata& meta, int bufferSizeInElements)
        : fileStream(fs),
        runMeta(meta),
        bufferSizeInElements(bufferSizeInElements),
        currentIndexInBuffer(0),
        elementsInBuffer(0), // 初始为空
        totalElementsRead(0)
    {
        buffer.resize(bufferSizeInElements);
    }

    // 从缓冲区获取下一个元素
    bool getNextItem(T& item) {
        // 检查内存缓冲区是否已空
        if (currentIndexInBuffer >= elementsInBuffer) {
            // 缓冲区已空，尝试从磁盘读取下一块
            if (!readBlock()) {
                // 如果 readBlock 失败，说明整个 Run 都读完了
                return false;
            }
        }

        // 从内存缓冲区提供元素
        item = buffer[currentIndexInBuffer++];
        return true;
    }
};

#endif // INPUT_BUFFER_H