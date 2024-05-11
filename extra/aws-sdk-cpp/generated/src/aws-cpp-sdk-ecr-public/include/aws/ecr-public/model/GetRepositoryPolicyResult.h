﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/ecr-public/ECRPublic_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Json
{
  class JsonValue;
} // namespace Json
} // namespace Utils
namespace ECRPublic
{
namespace Model
{
  class GetRepositoryPolicyResult
  {
  public:
    AWS_ECRPUBLIC_API GetRepositoryPolicyResult();
    AWS_ECRPUBLIC_API GetRepositoryPolicyResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_ECRPUBLIC_API GetRepositoryPolicyResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline const Aws::String& GetRegistryId() const{ return m_registryId; }

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline void SetRegistryId(const Aws::String& value) { m_registryId = value; }

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline void SetRegistryId(Aws::String&& value) { m_registryId = std::move(value); }

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline void SetRegistryId(const char* value) { m_registryId.assign(value); }

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRegistryId(const Aws::String& value) { SetRegistryId(value); return *this;}

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRegistryId(Aws::String&& value) { SetRegistryId(std::move(value)); return *this;}

    /**
     * <p>The registry ID that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRegistryId(const char* value) { SetRegistryId(value); return *this;}


    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline const Aws::String& GetRepositoryName() const{ return m_repositoryName; }

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline void SetRepositoryName(const Aws::String& value) { m_repositoryName = value; }

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline void SetRepositoryName(Aws::String&& value) { m_repositoryName = std::move(value); }

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline void SetRepositoryName(const char* value) { m_repositoryName.assign(value); }

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRepositoryName(const Aws::String& value) { SetRepositoryName(value); return *this;}

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRepositoryName(Aws::String&& value) { SetRepositoryName(std::move(value)); return *this;}

    /**
     * <p>The repository name that's associated with the request.</p>
     */
    inline GetRepositoryPolicyResult& WithRepositoryName(const char* value) { SetRepositoryName(value); return *this;}


    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline const Aws::String& GetPolicyText() const{ return m_policyText; }

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline void SetPolicyText(const Aws::String& value) { m_policyText = value; }

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline void SetPolicyText(Aws::String&& value) { m_policyText = std::move(value); }

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline void SetPolicyText(const char* value) { m_policyText.assign(value); }

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline GetRepositoryPolicyResult& WithPolicyText(const Aws::String& value) { SetPolicyText(value); return *this;}

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline GetRepositoryPolicyResult& WithPolicyText(Aws::String&& value) { SetPolicyText(std::move(value)); return *this;}

    /**
     * <p>The repository policy text that's associated with the repository. The policy
     * text will be in JSON format.</p>
     */
    inline GetRepositoryPolicyResult& WithPolicyText(const char* value) { SetPolicyText(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline GetRepositoryPolicyResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline GetRepositoryPolicyResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline GetRepositoryPolicyResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::String m_registryId;

    Aws::String m_repositoryName;

    Aws::String m_policyText;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace ECRPublic
} // namespace Aws