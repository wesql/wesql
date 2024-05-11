﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/waf/WAF_EXPORTS.h>
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
namespace WAF
{
namespace Model
{
  class UpdateRuleResult
  {
  public:
    AWS_WAF_API UpdateRuleResult();
    AWS_WAF_API UpdateRuleResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_WAF_API UpdateRuleResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline const Aws::String& GetChangeToken() const{ return m_changeToken; }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(const Aws::String& value) { m_changeToken = value; }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(Aws::String&& value) { m_changeToken = std::move(value); }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(const char* value) { m_changeToken.assign(value); }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline UpdateRuleResult& WithChangeToken(const Aws::String& value) { SetChangeToken(value); return *this;}

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline UpdateRuleResult& WithChangeToken(Aws::String&& value) { SetChangeToken(std::move(value)); return *this;}

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>UpdateRule</code> request. You can also use this value to query the status
     * of the request. For more information, see <a>GetChangeTokenStatus</a>.</p>
     */
    inline UpdateRuleResult& WithChangeToken(const char* value) { SetChangeToken(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline UpdateRuleResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline UpdateRuleResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline UpdateRuleResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::String m_changeToken;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace WAF
} // namespace Aws