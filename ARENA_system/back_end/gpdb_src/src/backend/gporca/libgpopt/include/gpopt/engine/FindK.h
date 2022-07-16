#ifndef ARENA_FINDK_H
#define ARENA_FINDK_H
#include <vector>
#include <cfloat>
#include <iostream>
#include <unordered_map>


//**************************************************/
// 全局变量，用于记录每个 plan 当前的最近距离
//      key: plan 在 vector 中的 index
//      value: min_dist
// 
// new_add：新加入的 plan 的 id
//**************************************************/
std::unordered_map<int, double> dist_record;
int new_add = 0;


//**************************************************/
//
// 用于找到 k 个最不一样的元素（k个不包含 best_plan)
// 
// plans 中的第一个元素代表 best_plan。 res 只存储相应的索引，是返回值
// 
//**************************************************/
template<class T>
void FindK(std::vector<T>& plans, std::vector<int>& res, std::size_t k)
{
    std::vector<bool> falive(true, plans.size());
    res.push_back(0);

    // 总数比需要找到的数量少
    if (plans.size() <= k)
    {
		for (std::size_t i = 1; i < plans.size(); ++i)
        {
            res.push_back(i);
        }
    }
    // 寻找 k 个 plan
    else
    {
        for (std::size_t i = 0; i < k; ++i)  // 迭代 k 次
        {
            // 本轮迭代需要加入的索引
            int index = -1;
            double max_min = DBL_MIN;
			double dist;

            // 第一次迭代，需要初始化 dist_record
            if (i == 0)
            {
                for (std::size_t j = 1; j < plans.size(); ++j)
                {
                    dist = plans[new_add].distance(plans[j]);
                    dist_record[j] = dist;
                    if (dist > max_min)
                    {
                        index = j;
                        max_min = dist;
                    }
                }
            }
            // 后面的多次迭代， dist_record 已经被初始化
            else
            {
                for (std::size_t j = 1; j < plans.size(); ++j)
                {
                    if (falive[j])
                    {
                        // 找到最小距离
                        dist = plans[new_add].distance(plans[j]);
                        if (dist < dist_record[j])
                        {
                            dist_record[j] = dist;
                        }
                        else
                        {
                            dist = dist_record[j];
                        }

                        // 大于当前已经找到的最小距离的最大值
                        if (dist > max_min)
                        {
                            index = j;
                            max_min = dist;
                        }
                    }
                }
            }

            res.push_back(index);
            falive[index] = false;  // 标注这个 index 已经被选出
            new_add = index;

        }
    }
}

#endif