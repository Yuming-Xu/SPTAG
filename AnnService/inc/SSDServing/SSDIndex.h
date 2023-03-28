// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <limits>
#include <sys/types.h>

#include "inc/Core/Common.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/SSDServing/Utils.h"

namespace SPTAG {
	namespace SSDServing {
		namespace SSDIndex {

            template <typename ValueType>
            ErrorCode OutputResult(const std::string& p_output, std::vector<QueryResult>& p_results, int p_resultNum)
            {
                if (!p_output.empty())
                {
                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(p_output.c_str(), std::ios::binary | std::ios::out)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed create file: %s\n", p_output.c_str());
                        return ErrorCode::FailedCreateFile;
                    }
                    int32_t i32Val = static_cast<int32_t>(p_results.size());
                    if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                        LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                        return ErrorCode::DiskIOFail;
                    }
                    i32Val = p_resultNum;
                    if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                        LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                        return ErrorCode::DiskIOFail;
                    }

                    float fVal = 0;
                    for (size_t i = 0; i < p_results.size(); ++i)
                    {
                        for (int j = 0; j < p_resultNum; ++j)
                        {
                            i32Val = p_results[i].GetResult(j)->VID;
                            if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                                LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                                return ErrorCode::DiskIOFail;
                            }

                            fVal = p_results[i].GetResult(j)->Dist;
                            if (ptr->WriteBinary(sizeof(fVal), reinterpret_cast<char*>(&fVal)) != sizeof(fVal)) {
                                LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                                return ErrorCode::DiskIOFail;
                            }
                        }
                    }
                }
                return ErrorCode::Success;
            }

            template<typename T, typename V>
            void PrintPercentiles(const std::vector<V>& p_values, std::function<T(const V&)> p_get, const char* p_format, bool reverse=false)
            {
                double sum = 0;
                std::vector<T> collects;
                collects.reserve(p_values.size());
                for (const auto& v : p_values)
                {
                    T tmp = p_get(v);
                    sum += tmp;
                    collects.push_back(tmp);
                }

                if (reverse) {
                    std::sort(collects.begin(), collects.end(), std::greater<T>());
                }
                else {
                    std::sort(collects.begin(), collects.end());
                }
                if (reverse) {
                    LOG(Helper::LogLevel::LL_Info, "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMin\n");
                }
                else {
                    LOG(Helper::LogLevel::LL_Info, "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMax\n");
                }

                std::string formatStr("%.3lf");
                for (int i = 1; i < 7; ++i)
                {
                    formatStr += '\t';
                    formatStr += p_format;
                }

                formatStr += '\n';

                LOG(Helper::LogLevel::LL_Info,
                    formatStr.c_str(),
                    sum / collects.size(),
                    collects[static_cast<size_t>(collects.size() * 0.50)],
                    collects[static_cast<size_t>(collects.size() * 0.90)],
                    collects[static_cast<size_t>(collects.size() * 0.95)],
                    collects[static_cast<size_t>(collects.size() * 0.99)],
                    collects[static_cast<size_t>(collects.size() * 0.999)],
                    collects[static_cast<size_t>(collects.size() - 1)]);
            }


            template <typename ValueType>
            void SearchSequential(SPANN::Index<ValueType>* p_index,
                int p_numThreads,
                std::vector<QueryResult>& p_results,
                std::vector<SPANN::SearchStats>& p_stats,
                int p_maxQueryCount, int p_internalResultNum)
            {
                int numQueries = min(static_cast<int>(p_results.size()), p_maxQueryCount);

                std::atomic_size_t queriesSent(0);

                std::vector<std::thread> threads;

                LOG(Helper::LogLevel::LL_Info, "Searching: numThread: %d, numQueries: %d.\n", p_numThreads, numQueries);

                Utils::StopW sw;

                auto func = [&]()
                {
                    Utils::StopW threadws;
                    size_t index = 0;
                    while (true)
                    {
                        index = queriesSent.fetch_add(1);
                        if (index < numQueries)
                        {
                            if ((index & ((1 << 14) - 1)) == 0)
                            {
                                LOG(Helper::LogLevel::LL_Info, "Sent %.2lf%%...\n", index * 100.0 / numQueries);
                            }

                            double startTime = threadws.getElapsedMs();
                            p_index->GetMemoryIndex()->SearchIndex(p_results[index]);
                            double endTime = threadws.getElapsedMs();
                            p_index->SearchDiskIndex(p_results[index], &(p_stats[index]));
                            double exEndTime = threadws.getElapsedMs();

                            p_stats[index].m_exLatency = exEndTime - endTime;
                            p_stats[index].m_totalLatency = p_stats[index].m_totalSearchLatency = exEndTime - startTime;
                        }
                        else
                        {
                            return;
                        }
                    }
                };

                for (int i = 0; i < p_numThreads; i++) { threads.emplace_back(func); }
                for (auto& thread : threads) { thread.join(); }

                double sendingCost = sw.getElapsedSec();

                LOG(Helper::LogLevel::LL_Info,
                    "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query count %u.\n",
                    sendingCost,
                    numQueries / sendingCost,
                    static_cast<uint32_t>(numQueries));

                for (int i = 0; i < numQueries; i++) { p_results[i].CleanQuantizedTarget(); }
            }

            void LoadUpdateMapping(std::string fileName, std::vector<SizeType>& reverseIndices)
            {
                LOG(Helper::LogLevel::LL_Info, "Loading %s\n", fileName.c_str());

                int vectorNum;

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(fileName.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open trace file: %s\n", fileName.c_str());
                    exit(1);
                }
                
                if (ptr->ReadBinary(4, (char *)&vectorNum) != 4) {
                    LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                }

                reverseIndices.clear();
                reverseIndices.resize(vectorNum);

                if (ptr->ReadBinary(vectorNum * 4, (char*)reverseIndices.data()) != vectorNum * 4) {
                    LOG(Helper::LogLevel::LL_Error, "update mapping Error!\n");
                    exit(1);
                }
            }

            void SaveUpdateMapping(std::string fileName, std::vector<SizeType>& reverseIndices, SizeType vectorNum)
            {
                LOG(Helper::LogLevel::LL_Info, "Saving %s\n", fileName.c_str());

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(fileName.c_str(), std::ios::out | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open trace file: %s\n", fileName.c_str());
                    exit(1);
                }
                
                if (ptr->WriteBinary(4, reinterpret_cast<char*>(&vectorNum)) != 4) {
                    LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                }

                for (int i = 0; i < vectorNum; i++) {
                    if (ptr->WriteBinary(4, reinterpret_cast<char*>(&reverseIndices[i])) != 4) {
                        LOG(Helper::LogLevel::LL_Error, "update mapping Error!\n");
                        exit(1);
                    }
                }
            }

            void SaveUpdateTrace(std::string fileName, SizeType& updateSize, std::vector<SizeType>& insertSet, std::vector<SizeType>& deleteSet)
            {
                LOG(Helper::LogLevel::LL_Info, "Saving %s\n", fileName.c_str());

                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(fileName.c_str(), std::ios::out | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open trace file: %s\n", fileName.c_str());
                    exit(1);
                }
                
                if (ptr->WriteBinary(4, reinterpret_cast<char*>(&updateSize)) != 4) {
                    LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                    exit(1);
                }

                for (int i = 0; i < updateSize; i++) {
                    if (ptr->WriteBinary(4, reinterpret_cast<char*>(&deleteSet[i])) != 4) {
                        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                        exit(1);
                    }
                }

                for (int i = 0; i < updateSize; i++) {
                    if (ptr->WriteBinary(4, reinterpret_cast<char*>(&insertSet[i])) != 4) {
                        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                        exit(1);
                    }
                }
            }

            void InitializeList(std::vector<SizeType>& current_list, std::vector<SizeType>& reserve_list, SPANN::Options& p_opts)
            {
                LOG(Helper::LogLevel::LL_Info, "Initialize list\n");
                if (p_opts.batch == 0) {
                    current_list.resize(p_opts.baseNum);
                    reserve_list.resize(p_opts.reserveNum);
                    for (int i = 0; i < p_opts.baseNum; i++) current_list[i] = i;
                    for (int i = 0; i < p_opts.reserveNum; i++) reserve_list[i] = i+p_opts.baseNum;
                } else {
                    LoadUpdateMapping(p_opts.currentListFileName + std::to_string(p_opts.batch-1), current_list);
                    LoadUpdateMapping(p_opts.reserveListFileName + std::to_string(p_opts.batch-1), reserve_list);
                }
            }

            template <typename ValueType>
            void GenerateMeta(SPANN::Index<ValueType>* p_index)
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                LOG(Helper::LogLevel::LL_Info, "Begin Generating MetaData\n");
                std::string metaDataFileName = "meta.bin";
                std::string metaIndexFileName = "metaIndex.bin";
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(metaDataFileName.c_str(), std::ios::out | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open meta file: %s\n", metaDataFileName.c_str());
                    exit(1);
                }
                auto index_ptr = f_createIO();
                if (index_ptr == nullptr || !index_ptr->Initialize(metaIndexFileName.c_str(), std::ios::out | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open metaIndex file: %s\n", metaIndexFileName.c_str());
                    exit(1);
                }
                uint64_t begin = 0;
                int vec_num = p_opts.baseNum;
                if (index_ptr->WriteBinary(4, reinterpret_cast<char*>(&vec_num)) != 4) {
                    LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                    exit(1);
                }
                if (index_ptr->WriteBinary(sizeof(uint64_t), reinterpret_cast<char*>(&begin)) != sizeof(uint64_t)) {
                    LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                    exit(1);
                }
                for (int i = 0; i < p_opts.baseNum; i++) {
                    std::string a = std::to_string(i);
                    if (ptr->WriteBinary(a.size(), a.data()) != a.size()) {
                        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                        exit(1);
                    }
                    begin += a.size();
                    if (index_ptr->WriteBinary(sizeof(uint64_t), reinterpret_cast<char*>(&begin)) != sizeof(uint64_t)) {
                        LOG(Helper::LogLevel::LL_Error, "vector Size Error!\n");
                        exit(1);
                    }
                }
    
            }

            template <typename ValueType>
            void GenerateTrace(SPANN::Index<ValueType>* p_index)
            {
                LOG(Helper::LogLevel::LL_Info, "Begin Generating Trace\n");
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::shared_ptr<VectorSet> vectorSet;
                std::vector<SizeType> current_list;
                std::vector<SizeType> reserve_list;
                InitializeList(current_list, reserve_list, p_opts);
                LOG(Helper::LogLevel::LL_Info, "Loading Vector Set\n");
                if (!p_opts.m_vectorPath.empty() && fileexists(p_opts.m_vectorPath.c_str())) {
                    std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_vectorType, p_opts.m_vectorDelimiter));
                    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
                    if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath))
                    {
                        vectorSet = vectorReader->GetVectorSet();
                        if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine) vectorSet->Normalize(4);
                        LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
                    }
                }
                LOG(Helper::LogLevel::LL_Info, "batch: %d, updateSize: %d\n", p_opts.batch, p_opts.updateSize);
                std::vector<SizeType> deleteList;
                std::vector<SizeType> insertList;
                deleteList.resize(p_opts.updateSize);
                insertList.resize(p_opts.updateSize);
                LOG(Helper::LogLevel::LL_Info, "Generate delete list\n");
                std::shuffle(current_list.begin(), current_list.end(), rg);
                for (int j = 0; j < p_opts.updateSize; j++) {
                    deleteList[j] = current_list[p_opts.baseNum-j-1];
                }
                current_list.resize(p_opts.baseNum-p_opts.updateSize);
                LOG(Helper::LogLevel::LL_Info, "Generate insert list\n");
                std::shuffle(reserve_list.begin(), reserve_list.end(), rg);
                for (int j = 0; j < p_opts.updateSize; j++) {
                    insertList[j] = reserve_list[p_opts.reserveNum-j-1];
                    current_list.push_back(reserve_list[p_opts.reserveNum-j-1]);
                }

                reserve_list.resize(p_opts.reserveNum-p_opts.updateSize);
                for (int j = 0; j < p_opts.updateSize; j++) {
                    reserve_list.push_back(deleteList[j]);
                }

                LOG(Helper::LogLevel::LL_Info, "Sorting list\n");
                std::sort(current_list.begin(), current_list.end());
                std::sort(reserve_list.begin(), reserve_list.end());

                SaveUpdateMapping(p_opts.currentListFileName + std::to_string(p_opts.batch), current_list, p_opts.baseNum);
                SaveUpdateMapping(p_opts.reserveListFileName + std::to_string(p_opts.batch), reserve_list, p_opts.reserveNum);

                SaveUpdateTrace(p_opts.traceFileName + std::to_string(p_opts.batch), p_opts.updateSize, insertList, deleteList);

                LOG(Helper::LogLevel::LL_Info, "Generate new dataset for truth\n");
                COMMON::Dataset<ValueType> newSample(0, p_opts.m_dim, p_index->m_iDataBlockSize, p_index->m_iDataCapacity);

                for (int i = 0; i < p_opts.baseNum; i++) {
                    newSample.AddBatch((ValueType*)(vectorSet->GetVector(current_list[i])), 1);
                }

                LOG(Helper::LogLevel::LL_Info, "Saving Truth\n");
                newSample.Save(p_opts.newDataSetFileName + std::to_string(p_opts.batch));
            }

            template <typename ValueType>
            void ConvertTruth(SPANN::Index<ValueType>* p_index)
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::vector<SizeType> current_list;
                LoadUpdateMapping(p_opts.currentListFileName + std::to_string(p_opts.batch), current_list);
                int K = p_opts.m_resultNum;
                int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;

                int numQueries = p_opts.m_querySize;

                std::string truthFile = p_opts.m_truthPath + std::to_string(p_opts.batch);
                std::vector<std::vector<SizeType>> truth;
                LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");
                truth.resize(numQueries);
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(truthFile.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthFile.c_str());
                    exit(1);
                }
                int originalK = truthK;
                if (ptr->TellP() == 0) {
                    int row;
                    if (ptr->ReadBinary(4, (char*)&row) != 4 || ptr->ReadBinary(4, (char*)&originalK) != 4) {
                        LOG(Helper::LogLevel::LL_Error, "Fail to read truth file!\n");
                        exit(1);
                    }
                }
                for (int i = 0; i < numQueries; i++)
                {
                    truth[i].resize(originalK);
                    if (ptr->ReadBinary(4 * originalK, (char*)truth[i].data()) != 4 * originalK) {
                        LOG(Helper::LogLevel::LL_Error, "Truth number(%d) and query number(%d) are not match!\n", i, numQueries);
                        exit(1);
                    }
                }

                LOG(Helper::LogLevel::LL_Info, "ChangeMapping & Writing\n");
                std::string truthFileAfter = p_opts.m_truthPath + "_after" + std::to_string(p_opts.batch);
                ptr = SPTAG::f_createIO();
                if (ptr == nullptr || !ptr->Initialize(truthFileAfter.c_str(), std::ios::out | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Fail to create the file:%s\n", truthFileAfter.c_str());
                    exit(1);
                }
                ptr->WriteBinary(4, (char*)&numQueries);
                ptr->WriteBinary(4, (char*)&K);
                for (int i = 0; i < numQueries; i++) {
                    for (int j = 0; j < originalK ; j++) {
                        if (ptr->WriteBinary(4, (char*)(&current_list[truth[i][j]])) != 4) {
                            LOG(Helper::LogLevel::LL_Error, "Fail to write the truth file!\n");
                            exit(1);
                        }
                    }
                }
            }

            void LoadTruth(SPANN::Options& p_opts, std::vector<std::set<SizeType>>& truth, int numQueries, std::string truthfilename, int truthK)
            {
                auto ptr = f_createIO();
                LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...: %s\n", truthfilename.c_str());
                
                if (ptr == nullptr || !ptr->Initialize(truthfilename.c_str(), std::ios::in | std::ios::binary)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthfilename.c_str());
                    exit(1);
                }
                LOG(Helper::LogLevel::LL_Error, "K: %d, TruthResultNum: %d\n", p_opts.m_resultNum, p_opts.m_truthResultNum);    
                COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, p_opts.m_truthResultNum, p_opts.m_resultNum, p_opts.m_truthType);
                char tmp[4];
                if (ptr->ReadBinary(4, tmp) == 4) {
                    LOG(Helper::LogLevel::LL_Error, "Truth number is larger than query number(%d)!\n", numQueries);
                }
            }

            std::shared_ptr<VectorSet>  LoadVectorSet(SPANN::Options& p_opts, int numThreads)
            {
                std::shared_ptr<VectorSet> vectorSet;
                LOG(Helper::LogLevel::LL_Info, "Start loading VectorSet...\n");
                if (!p_opts.m_vectorPath.empty() && fileexists(p_opts.m_vectorPath.c_str())) {
                    std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_vectorType, p_opts.m_vectorDelimiter));
                    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
                    if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath))
                    {
                        vectorSet = vectorReader->GetVectorSet();
                        if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine) vectorSet->Normalize(numThreads);
                        LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
                    }
                }
                return vectorSet;
            }

            std::shared_ptr<VectorSet> LoadQuerySet(SPANN::Options& p_opts)
            {
                LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
                std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_queryType, p_opts.m_queryDelimiter));
                auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
                if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath))
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                    exit(1);
                }
                return queryReader->GetVectorSet();
            }

            void LoadOutputResult(SPANN::Options& p_opts, std::vector<std::vector<SizeType>>& ids,
                std::vector<std::vector<float>>& dists) 
            {
                auto ptr = f_createIO();
                if (ptr == nullptr || !ptr->Initialize(p_opts.m_searchResult.c_str(), std::ios::binary | std::ios::in)) {
                    LOG(Helper::LogLevel::LL_Error, "Failed open file: %s\n", p_opts.m_searchResult.c_str());
                    exit(1);
                }
                int32_t NumQuerys;
                if (ptr->ReadBinary(sizeof(NumQuerys), (char*)&NumQuerys) != sizeof(NumQuerys)) {
                    LOG(Helper::LogLevel::LL_Error, "Fail to read result file!\n");
                    exit(1);
                }
                int32_t resultNum;
                if (ptr->ReadBinary(sizeof(resultNum), (char*)&resultNum) != sizeof(resultNum)) {
                    LOG(Helper::LogLevel::LL_Error, "Fail to read result file!\n");
                    exit(1);
                }


                for (size_t i = 0; i < NumQuerys; ++i)
                {
                    std::vector<SizeType> tempVec_id;
                    std::vector<float> tempVec_dist;
                    for (int j = 0; j < resultNum; ++j)
                    {
                        int32_t i32Val;
                        if (ptr->ReadBinary(sizeof(i32Val), (char*)&i32Val) != sizeof(i32Val)) {
                            LOG(Helper::LogLevel::LL_Error, "Fail to read result file!\n");
                            exit(1);
                        }

                        float fVal;
                        if (ptr->ReadBinary(sizeof(fVal), (char*)&fVal) != sizeof(fVal)) {
                            LOG(Helper::LogLevel::LL_Error, "Fail to read result file!\n");
                            exit(1);
                        }
                        tempVec_id.push_back(i32Val);
                        tempVec_dist.push_back(fVal);
                    }
                    ids.push_back(tempVec_id);
                    dists.push_back(tempVec_dist);
                }
            }

            template <typename ValueType>
            void CallRecall(SPANN::Index<ValueType>* p_index)      
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::string truthfile = p_opts.m_truthPath;
                std::vector<std::set<SizeType>> truth;
                int truthK = p_opts.m_resultNum;
                int K = p_opts.m_resultNum;
                auto vectorSet = LoadVectorSet(p_opts, 10);
                auto querySet = LoadQuerySet(p_opts);
                int NumQuerys = querySet->Count();
                LoadTruth(p_opts, truth, NumQuerys, truthfile, truthK);
                std::vector<std::vector<SizeType>> ids;
                std::vector<std::vector<float>> dists;
                VectorIndex* index = (p_index->GetMemoryIndex()).get();

                LoadOutputResult(p_opts,ids, dists);

                float meanrecall = 0, minrecall = MaxDist, maxrecall = 0, stdrecall = 0;
                std::vector<float> thisrecall(NumQuerys, 0);
                std::unique_ptr<bool[]> visited(new bool[K]);
                LOG(Helper::LogLevel::LL_Info, "Start Calculating Recall\n");
                for (SizeType i = 0; i < NumQuerys; i++)
                {
                    memset(visited.get(), 0, K * sizeof(bool));
                    for (SizeType id : truth[i])
                    {
                        for (int j = 0; j < K; j++)
                        {
                            if (visited[j] || ids[i][j] < 0) continue;
                            // if (i == 0) LOG(Helper::LogLevel::LL_Info, "calculating %d, ids: %d, dist:%f, groundtruth: %d\n", i, ids[i][j], dists[i][j], id);
                            if (vectorSet != nullptr) {
                                float dist = dists[i][j];
                                float truthDist = COMMON::DistanceUtils::ComputeDistance((const ValueType*)querySet->GetVector(i), (const ValueType*)vectorSet->GetVector(id), vectorSet->Dimension(), SPTAG::DistCalcMethod::L2);
                                // if (i == 0) LOG(Helper::LogLevel::LL_Info, "truthDist: %f\n", truthDist);
                                if (fabs(dist - truthDist) < Epsilon * (dist + Epsilon)) {
                                    thisrecall[i] += 1;
                                    visited[j] = true;
                                    break;
                                }
                            }
                        }
                    }
                    thisrecall[i] /= truthK;
                    meanrecall += thisrecall[i];
                    if (thisrecall[i] < minrecall) minrecall = thisrecall[i];
                    if (thisrecall[i] > maxrecall) maxrecall = thisrecall[i];
                }
                meanrecall /= NumQuerys;
                for (SizeType i = 0; i < NumQuerys; i++)
                {
                    stdrecall += (thisrecall[i] - meanrecall) * (thisrecall[i] - meanrecall);
                }
                stdrecall = std::sqrt(stdrecall / NumQuerys);

                LOG(Helper::LogLevel::LL_Info, "stdrecall: %.6lf, maxrecall: %.2lf, minrecall: %.2lf\n", stdrecall, maxrecall, minrecall);

                LOG(Helper::LogLevel::LL_Info, "\nRecall Distribution:\n");
                PrintPercentiles<float, float>(thisrecall,
                    [](const float recall) -> float
                    {
                        return recall;
                    },
                    "%.3lf", true);

                LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f\n", K, truthK, meanrecall);
            }

            template <typename ValueType>
            void generateSet(SPANN::Index<ValueType>* p_index)
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::shared_ptr<VectorSet> vectorSet;

                if (!p_opts.m_vectorPath.empty() && fileexists(p_opts.m_vectorPath.c_str())) {
                    std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_vectorType, p_opts.m_vectorDelimiter));
                    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
                    if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath))
                    {
                        vectorSet = vectorReader->GetVectorSet();
                        if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine) vectorSet->Normalize(10);
                        LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
                    }
                }

                std::string headIDFile = p_opts.m_headIDFile;

                std::vector<SizeType> headIDmap;
                int headNum = p_opts.m_vectorSize;

                headIDmap.resize(headNum);

                auto fp = SPTAG::f_createIO();
                if (fp == nullptr || !fp->Initialize(headIDFile.c_str(), std::ios::binary | std::ios::in)) {
                    exit(1);
                }

                // if (fp->ReadBinary(sizeof(std::uint64_t) * headNum, reinterpret_cast<char*>(headIDmap.get())) != sizeof(std::uint64_t) * headNum) {
                //     LOG(Helper::LogLevel::LL_Error, "Fail to read headID file!\n");
                //     exit(1);
                // }

                if (fp->ReadBinary(sizeof(SizeType) * headNum, (char*)headIDmap.data()) != sizeof(SizeType) * headNum) {
                    LOG(Helper::LogLevel::LL_Error, "Fail to read headID file!\n");
                    exit(1);
                }

                LOG(Helper::LogLevel::LL_Info, "Generating\n");
                COMMON::Dataset<ValueType> newSample(0, p_opts.m_dim, p_index->m_iDataBlockSize, p_index->m_iDataCapacity);
                for (int i = 0; i < headNum; i++) {
                    newSample.AddBatch((ValueType*)(vectorSet->GetVector(headIDmap[i])), 1);
                }
                LOG(Helper::LogLevel::LL_Info, "Saving\n");
                newSample.Save(p_opts.m_headVectorFile);
                
            }


            template <typename ValueType>
            void Search(SPANN::Index<ValueType>* p_index)
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::string outputFile = p_opts.m_searchResult;
                std::string truthFile = p_opts.m_truthPath;
                std::string warmupFile = p_opts.m_warmupPath;

                if (p_index->m_pQuantizer)
                {
                   p_index->m_pQuantizer->SetEnableADC(p_opts.m_enableADC);
                }

                if (!p_opts.m_logFile.empty())
                {
                    g_pLogger.reset(new Helper::FileLogger(Helper::LogLevel::LL_Info, p_opts.m_logFile.c_str()));
                }
                int numThreads = p_opts.m_iSSDNumberOfThreads;
                int internalResultNum = p_opts.m_searchInternalResultNum;
                int K = p_opts.m_resultNum;
                int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;

                if (!warmupFile.empty())
                {
                    LOG(Helper::LogLevel::LL_Info, "Start loading warmup query set...\n");
                    std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_warmupType, p_opts.m_warmupDelimiter));
                    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
                    if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_warmupPath))
                    {
                        LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                        exit(1);
                    }
                    auto warmupQuerySet = queryReader->GetVectorSet();
                    int warmupNumQueries = warmupQuerySet->Count();

                    std::vector<QueryResult> warmupResults(warmupNumQueries, QueryResult(NULL, max(K, internalResultNum), false));
                    std::vector<SPANN::SearchStats> warmpUpStats(warmupNumQueries);
                    for (int i = 0; i < warmupNumQueries; ++i)
                    {
                        (*((COMMON::QueryResultSet<ValueType>*)&warmupResults[i])).SetTarget(reinterpret_cast<ValueType*>(warmupQuerySet->GetVector(i)), p_index->m_pQuantizer);
                        warmupResults[i].Reset();
                    }

                    LOG(Helper::LogLevel::LL_Info, "Start warmup...\n");
                    SearchSequential(p_index, numThreads, warmupResults, warmpUpStats, p_opts.m_queryCountLimit, internalResultNum);
                    LOG(Helper::LogLevel::LL_Info, "\nFinish warmup...\n");
                }

                LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
                std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_queryType, p_opts.m_queryDelimiter));
                auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
                if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath))
                {
                    LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                    exit(1);
                }
                auto querySet = queryReader->GetVectorSet();
                int numQueries = querySet->Count();

                std::vector<QueryResult> results(numQueries, QueryResult(NULL, max(K, internalResultNum), false));
                std::vector<SPANN::SearchStats> stats(numQueries);
                for (int i = 0; i < numQueries; ++i)
                {
                    (*((COMMON::QueryResultSet<ValueType>*)&results[i])).SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)), p_index->m_pQuantizer);
                    results[i].Reset();
                }


                LOG(Helper::LogLevel::LL_Info, "Start ANN Search...\n");

                SearchSequential(p_index, numThreads, results, stats, p_opts.m_queryCountLimit, internalResultNum);

                LOG(Helper::LogLevel::LL_Info, "\nFinish ANN Search...\n");

                std::shared_ptr<VectorSet> vectorSet;

                if (!p_opts.m_vectorPath.empty() && fileexists(p_opts.m_vectorPath.c_str())) {
                    std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_vectorType, p_opts.m_vectorDelimiter));
                    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
                    if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath))
                    {
                        vectorSet = vectorReader->GetVectorSet();
                        if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine) vectorSet->Normalize(numThreads);
                        LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
                    }
                }

                if (p_opts.m_rerank > 0 && vectorSet != nullptr) {
                    LOG(Helper::LogLevel::LL_Info, "\n Begin rerank...\n");
                    for (int i = 0; i < results.size(); i++)
                    {
                        for (int j = 0; j < K; j++)
                        {
                            if (results[i].GetResult(j)->VID < 0) continue;
                            results[i].GetResult(j)->Dist = COMMON::DistanceUtils::ComputeDistance((const ValueType*)querySet->GetVector(i),
                                (const ValueType*)vectorSet->GetVector(results[i].GetResult(j)->VID), querySet->Dimension(), p_opts.m_distCalcMethod);
                        }
                        BasicResult* re = results[i].GetResults();
                        std::sort(re, re + K, COMMON::Compare);
                    }
                    K = p_opts.m_rerank;
                }

                float recall = 0, MRR = 0;
                std::vector<std::set<SizeType>> truth;
                if (!truthFile.empty())
                {
                    LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");

                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(truthFile.c_str(), std::ios::in | std::ios::binary)) {
                        LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthFile.c_str());
                        exit(1);
                    }
                    int originalK = truthK;
                    COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK, p_opts.m_truthType);
                    char tmp[4];
                    if (ptr->ReadBinary(4, tmp) == 4) {
                        LOG(Helper::LogLevel::LL_Error, "Truth number is larger than query number(%d)!\n", numQueries);
                    }

                    recall = COMMON::TruthSet::CalculateRecall<ValueType>((p_index->GetMemoryIndex()).get(), results, truth, K, truthK, querySet, vectorSet, numQueries, nullptr, false, &MRR);
                    LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
                }

                LOG(Helper::LogLevel::LL_Info, "\nEx Elements Count:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalListElementsCount;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nHead Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalSearchLatency - ss.m_exLatency;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nEx Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_exLatency;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nTotal Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalSearchLatency;
                    },
                    "%.3lf");

                LOG(Helper::LogLevel::LL_Info, "\nTotal Disk Page Access Distribution:\n");
                PrintPercentiles<int, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> int
                    {
                        return ss.m_diskAccessCount;
                    },
                    "%4d");

                LOG(Helper::LogLevel::LL_Info, "\nTotal Disk IO Distribution:\n");
                PrintPercentiles<int, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> int
                    {
                        return ss.m_diskIOCount;
                    },
                    "%4d");

                LOG(Helper::LogLevel::LL_Info, "\n");

                if (!outputFile.empty())
                {
                    LOG(Helper::LogLevel::LL_Info, "Start output to %s\n", outputFile.c_str());
                    OutputResult<ValueType>(outputFile, results, K);
                }

                LOG(Helper::LogLevel::LL_Info,
                    "Recall@%d: %f MRR@%d: %f\n", K, recall, K, MRR);

                LOG(Helper::LogLevel::LL_Info, "\n");

                if (p_opts.m_recall_analysis) {
                    LOG(Helper::LogLevel::LL_Info, "Start recall analysis...\n");

                    std::shared_ptr<VectorIndex> headIndex = p_index->GetMemoryIndex();
                    SizeType sampleSize = numQueries < 100 ? numQueries : 100;
                    SizeType sampleK = headIndex->GetNumSamples() < 1000 ? headIndex->GetNumSamples() : 1000;
                    float sampleE = 1e-6f;

                    std::vector<SizeType> samples(sampleSize, 0);
                    std::vector<float> queryHeadRecalls(sampleSize, 0);
                    std::vector<float> truthRecalls(sampleSize, 0);
                    std::vector<int> shouldSelect(sampleSize, 0);
                    std::vector<int> shouldSelectLong(sampleSize, 0);
                    std::vector<int> nearQueryHeads(sampleSize, 0);
                    std::vector<int> annNotFound(sampleSize, 0);
                    std::vector<int> rngRule(sampleSize, 0);
                    std::vector<int> postingCut(sampleSize, 0);
                    for (int i = 0; i < sampleSize; i++) samples[i] = COMMON::Utils::rand(numQueries);

#pragma omp parallel for schedule(dynamic)
                    for (int i = 0; i < sampleSize; i++)
                    {
                        COMMON::QueryResultSet<ValueType> queryANNHeads((const ValueType*)(querySet->GetVector(samples[i])), max(K, internalResultNum));
                        headIndex->SearchIndex(queryANNHeads);
                        float queryANNHeadsLongestDist = queryANNHeads.GetResult(internalResultNum - 1)->Dist;

                        COMMON::QueryResultSet<ValueType> queryBFHeads((const ValueType*)(querySet->GetVector(samples[i])), max(sampleK, internalResultNum));
                        for (SizeType y = 0; y < headIndex->GetNumSamples(); y++)
                        {
                            float dist = headIndex->ComputeDistance(queryBFHeads.GetQuantizedTarget(), headIndex->GetSample(y));
                            queryBFHeads.AddPoint(y, dist);
                        }
                        queryBFHeads.SortResult();

                        {
                            std::vector<bool> visited(internalResultNum, false);
                            for (SizeType y = 0; y < internalResultNum; y++)
                            {
                                for (SizeType z = 0; z < internalResultNum; z++)
                                {
                                    if (visited[z]) continue;

                                    if (fabs(queryANNHeads.GetResult(z)->Dist - queryBFHeads.GetResult(y)->Dist) < sampleE)
                                    {
                                        queryHeadRecalls[i] += 1;
                                        visited[z] = true;
                                        break;
                                    }
                                }
                            }
                        }

                        std::map<int, std::set<int>> tmpFound; // headID->truths
                        p_index->DebugSearchDiskIndex(queryBFHeads, internalResultNum, sampleK, nullptr, &truth[samples[i]], &tmpFound);

                        for (SizeType z = 0; z < K; z++) {
                            truthRecalls[i] += truth[samples[i]].count(queryBFHeads.GetResult(z)->VID);
                        }

                        for (SizeType z = 0; z < K; z++) {
                            truth[samples[i]].erase(results[samples[i]].GetResult(z)->VID);
                        }

                        for (std::map<int, std::set<int>>::iterator it = tmpFound.begin(); it != tmpFound.end(); it++) {
                            float q2truthposting = headIndex->ComputeDistance(querySet->GetVector(samples[i]), headIndex->GetSample(it->first));
                            for (auto vid : it->second) {
                                if (!truth[samples[i]].count(vid)) continue;

                                if (q2truthposting < queryANNHeadsLongestDist) shouldSelect[i] += 1;
                                else {
                                    shouldSelectLong[i] += 1;

                                    std::set<int> nearQuerySelectedHeads;
                                    float v2vhead = headIndex->ComputeDistance(vectorSet->GetVector(vid), headIndex->GetSample(it->first));
                                    for (SizeType z = 0; z < internalResultNum; z++) {
                                        if (queryANNHeads.GetResult(z)->VID < 0) break;
                                        float v2qhead = headIndex->ComputeDistance(vectorSet->GetVector(vid), headIndex->GetSample(queryANNHeads.GetResult(z)->VID));
                                        if (v2qhead < v2vhead) {
                                            nearQuerySelectedHeads.insert(queryANNHeads.GetResult(z)->VID);
                                        }
                                    }
                                    if (nearQuerySelectedHeads.size() == 0) continue;

                                    nearQueryHeads[i] += 1;

                                    COMMON::QueryResultSet<ValueType> annTruthHead((const ValueType*)(vectorSet->GetVector(vid)), p_opts.m_debugBuildInternalResultNum);
                                    headIndex->SearchIndex(annTruthHead);

                                    bool found = false;
                                    for (SizeType z = 0; z < annTruthHead.GetResultNum(); z++) {
                                        if (nearQuerySelectedHeads.count(annTruthHead.GetResult(z)->VID)) {
                                            found = true;
                                            break;
                                        }
                                    }

                                    if (!found) {
                                        annNotFound[i] += 1;
                                        continue;
                                    }

                                    // RNG rule and posting cut
                                    std::set<int> replicas;
                                    for (SizeType z = 0; z < annTruthHead.GetResultNum() && replicas.size() < p_opts.m_replicaCount; z++) {
                                        BasicResult* item = annTruthHead.GetResult(z);
                                        if (item->VID < 0) break;

                                        bool good = true;
                                        for (auto r : replicas) {
                                            if (p_opts.m_rngFactor * headIndex->ComputeDistance(headIndex->GetSample(r), headIndex->GetSample(item->VID)) < item->Dist) {
                                                good = false;
                                                break;
                                            }
                                        }
                                        if (good) replicas.insert(item->VID);
                                    }

                                    found = false;
                                    for (auto r : nearQuerySelectedHeads) {
                                        if (replicas.count(r)) {
                                            found = true;
                                            break;
                                        }
                                    }

                                    if (found) postingCut[i] += 1;
                                    else rngRule[i] += 1;
                                }
                            }
                        }
                    }
                    float headacc = 0, truthacc = 0, shorter = 0, longer = 0, lost = 0, buildNearQueryHeads = 0, buildAnnNotFound = 0, buildRNGRule = 0, buildPostingCut = 0;
                    for (int i = 0; i < sampleSize; i++) {
                        headacc += queryHeadRecalls[i];
                        truthacc += truthRecalls[i];

                        lost += shouldSelect[i] + shouldSelectLong[i];
                        shorter += shouldSelect[i];
                        longer += shouldSelectLong[i];

                        buildNearQueryHeads += nearQueryHeads[i];
                        buildAnnNotFound += annNotFound[i];
                        buildRNGRule += rngRule[i];
                        buildPostingCut += postingCut[i];
                    }

                    LOG(Helper::LogLevel::LL_Info, "Query head recall @%d:%f.\n", internalResultNum, headacc / sampleSize / internalResultNum);
                    LOG(Helper::LogLevel::LL_Info, "BF top %d postings truth recall @%d:%f.\n", sampleK, truthK, truthacc / sampleSize / truthK);

                    LOG(Helper::LogLevel::LL_Info,
                        "Percent of truths in postings have shorter distance than query selected heads: %f percent\n",
                        shorter / lost * 100);
                    LOG(Helper::LogLevel::LL_Info,
                        "Percent of truths in postings have longer distance than query selected heads: %f percent\n",
                        longer / lost * 100);


                    LOG(Helper::LogLevel::LL_Info,
                        "\tPercent of truths no shorter distance in query selected heads: %f percent\n",
                        (longer - buildNearQueryHeads) / lost * 100);
                    LOG(Helper::LogLevel::LL_Info,
                        "\tPercent of truths exists shorter distance in query selected heads: %f percent\n",
                        buildNearQueryHeads / lost * 100);

                    LOG(Helper::LogLevel::LL_Info,
                        "\t\tRNG rule ANN search loss: %f percent\n", buildAnnNotFound / lost * 100);
                    LOG(Helper::LogLevel::LL_Info,
                        "\t\tPosting cut loss: %f percent\n", buildPostingCut / lost * 100);
                    LOG(Helper::LogLevel::LL_Info,
                        "\t\tRNG rule loss: %f percent\n", buildRNGRule / lost * 100);
                }
            }
		}
	}
}
