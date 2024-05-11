﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/identitystore/IdentityStore_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Json
{
  class JsonValue;
  class JsonView;
} // namespace Json
} // namespace Utils
namespace IdentityStore
{
namespace Model
{

  /**
   * <p>You do not have sufficient access to perform this action.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/identitystore-2020-06-15/AccessDeniedException">AWS
   * API Reference</a></p>
   */
  class AccessDeniedException
  {
  public:
    AWS_IDENTITYSTORE_API AccessDeniedException();
    AWS_IDENTITYSTORE_API AccessDeniedException(Aws::Utils::Json::JsonView jsonValue);
    AWS_IDENTITYSTORE_API AccessDeniedException& operator=(Aws::Utils::Json::JsonView jsonValue);
    AWS_IDENTITYSTORE_API Aws::Utils::Json::JsonValue Jsonize() const;


    
    inline const Aws::String& GetMessage() const{ return m_message; }

    
    inline bool MessageHasBeenSet() const { return m_messageHasBeenSet; }

    
    inline void SetMessage(const Aws::String& value) { m_messageHasBeenSet = true; m_message = value; }

    
    inline void SetMessage(Aws::String&& value) { m_messageHasBeenSet = true; m_message = std::move(value); }

    
    inline void SetMessage(const char* value) { m_messageHasBeenSet = true; m_message.assign(value); }

    
    inline AccessDeniedException& WithMessage(const Aws::String& value) { SetMessage(value); return *this;}

    
    inline AccessDeniedException& WithMessage(Aws::String&& value) { SetMessage(std::move(value)); return *this;}

    
    inline AccessDeniedException& WithMessage(const char* value) { SetMessage(value); return *this;}


    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline bool RequestIdHasBeenSet() const { return m_requestIdHasBeenSet; }

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline void SetRequestId(const Aws::String& value) { m_requestIdHasBeenSet = true; m_requestId = value; }

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline void SetRequestId(Aws::String&& value) { m_requestIdHasBeenSet = true; m_requestId = std::move(value); }

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline void SetRequestId(const char* value) { m_requestIdHasBeenSet = true; m_requestId.assign(value); }

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline AccessDeniedException& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline AccessDeniedException& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    /**
     * <p>The identifier for each request. This value is a globally unique ID that is
     * generated by the identity store service for each sent request, and is then
     * returned inside the exception if the request fails.</p>
     */
    inline AccessDeniedException& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::String m_message;
    bool m_messageHasBeenSet = false;

    Aws::String m_requestId;
    bool m_requestIdHasBeenSet = false;
  };

} // namespace Model
} // namespace IdentityStore
} // namespace Aws