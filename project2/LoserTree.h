#ifndef LOSER_TREE_H
#define LOSER_TREE_H

#include <vector>
#include <limits>
#include <stdexcept>

// 数据元素包装器，包含归并段 ID (Run ID)
template <typename T>
struct RunNode {
    T value;
    int runID;

    // 默认构造函数：初始化为最大值（哨兵）
    RunNode() : value(std::numeric_limits<T>::max()), runID(std::numeric_limits<int>::max()) {}

    RunNode(T v, int id) : value(v), runID(id) {}

    // 不等运算符
    bool operator!=(const RunNode& other) const {
        return runID != other.runID || value != other.value;
    }
};

template <typename T>
class LoserTree {
private:
    std::vector<int> tree;          // 内部节点：存储败者的索引
    std::vector<RunNode<T>> leaves; // 叶子节点：存储实际数据
    int k;

    // 哨兵：表示无穷大（最大值，最大 RunID）
    RunNode<T> SENTINEL;

    // 辅助函数：如果 playerA 输给 playerB 则返回 true
    // 我们需要最小树，所以“较大者” = 败者
    bool isLoser(const RunNode<T>& playerA, const RunNode<T>& playerB) {
        if (playerA.runID != playerB.runID) {
            return playerA.runID > playerB.runID; // RunID 大的输
        }
        return playerA.value > playerB.value; // RunID 相同，数值大的输
    }

    // 从叶子节点 playerIndex 开始重赛
    void replay(int playerIndex) {
        int parent = (playerIndex + k) / 2;
        int currentWinner = playerIndex;

        while (parent > 0) {
            // 比较当前胜者与父节点存储的败者
            // 如果当前胜者“较大”（isLoser 返回 true），则当前胜者输了
            if (isLoser(leaves[currentWinner], leaves[tree[parent]])) {
                // 交换：当前胜者（变为败者）留在父节点，原父节点内容（新胜者）继续向上
                int temp = tree[parent];
                tree[parent] = currentWinner;
                currentWinner = temp;
            }
            //不管输赢，最终胜者都继续向上比较
            parent /= 2;
        }
        // 最终胜者存储在 tree[0]
        tree[0] = currentWinner;
    }

public:
    // 构造函数
    LoserTree(int k) : k(k) {
        if (k <= 0) throw std::invalid_argument("k must be > 0");

        tree.resize(k);
        leaves.resize(k + 1); // +1 用于在 k 位置存放哨兵

        // 初始化哨兵
        SENTINEL.value = std::numeric_limits<T>::max();
        SENTINEL.runID = std::numeric_limits<int>::max();
        leaves[k] = SENTINEL;
    }

    // 使用数据初始化败者树
    void initialize(const std::vector<T>& initialData) {
        // 1. 填充叶子节点：填入数据（初始 RunID = 1）或哨兵
        for (int i = 0; i < k; ++i) {
            if (i < initialData.size()) {
                leaves[i] = RunNode<T>(initialData[i], 1);
            }
            else {
                leaves[i] = SENTINEL;
            }
        }
        leaves[k] = SENTINEL;

        // 2. 重置所有内部节点指向哨兵索引 (k)
        // 这在构建过程中标记它们为“空”
        for (int i = 0; i < k; ++i) tree[i] = k;

        // 3. 构建树（Knuth 算法 / 锦标赛构建）
        for (int i = k - 1; i >= 0; --i) {
            int current = i;
            int parent = (i + k) / 2;

            while (parent > 0) {
                if (tree[parent] == k) {
                    // 访问者逻辑（第一个到达者）：
                    // 如果该节点为空（指向哨兵），则停在这里。
                    // 该节点成为“第一条分支的胜者”，等待对手。
                    tree[parent] = current;
                    break;
                }
                else {
                    // 访问者逻辑（第二个到达者）：
                    // 已经有选手在这里等待。比赛开始！
                    int other = tree[parent];
                    if (isLoser(leaves[current], leaves[other])) {
                        // 当前选手输了。留在父节点。对手（胜者）继续向上。
                        tree[parent] = current;
                        current = other;
                    }
                    else {
                        // 当前选手赢了。对手留下（作为败者）。当前选手继续向上。
                        // tree[parent] 保持不变。
                    }
                    parent /= 2;
                }
            }
            // 如果冒泡到了顶部，记录全局胜者
            if (parent == 0) {
                tree[0] = current;
            }
        }
    }

    // 获取当前胜者（最小值）
    RunNode<T> getWinner() const {
        return leaves[tree[0]];
    }

    // 替换胜者为新数值和新 RunID，然后重赛
    void replaceWinner(T newValue, int newRunID) {
        int idx = tree[0];
        leaves[idx].value = newValue;
        leaves[idx].runID = newRunID;
        replay(idx);
    }

    // 将胜者标记为哨兵（用于输入耗尽时）
    void setWinnerToSentinel() {
        int idx = tree[0];
        leaves[idx] = SENTINEL;
        replay(idx);
    }
};

#endif