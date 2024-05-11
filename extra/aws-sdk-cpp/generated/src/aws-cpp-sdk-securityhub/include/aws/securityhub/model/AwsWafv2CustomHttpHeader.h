﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/securityhub/SecurityHub_EXPORTS.h>
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
namespace SecurityHub
{
namespace Model
{

  /**
   * <p> A custom header for custom request and response handling. </p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/securityhub-2018-10-26/AwsWafv2CustomHttpHeader">AWS
   * API Reference</a></p>
   */
  class AwsWafv2CustomHttpHeader
  {
  public:
    AWS_SECURITYHUB_API AwsWafv2CustomHttpHeader();
    AWS_SECURITYHUB_API AwsWafv2CustomHttpHeader(Aws::Utils::Json::JsonView jsonValue);
    AWS_SECURITYHUB_API AwsWafv2CustomHttpHeader& operator=(Aws::Utils::Json::JsonView jsonValue);
    AWS_SECURITYHUB_API Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p> The name of the custom header. </p>
     */
    inline const Aws::String& GetName() const{ return m_name; }

    /**
     * <p> The name of the custom header. </p>
     */
    inline bool NameHasBeenSet() const { return m_nameHasBeenSet; }

    /**
     * <p> The name of the custom header. </p>
     */
    inline void SetName(const Aws::String& value) { m_nameHasBeenSet = true; m_name = value; }

    /**
     * <p> The name of the custom header. </p>
     */
    inline void SetName(Aws::String&& value) { m_nameHasBeenSet = true; m_name = std::move(value); }

    /**
     * <p> The name of the custom header. </p>
     */
    inline void SetName(const char* value) { m_nameHasBeenSet = true; m_name.assign(value); }

    /**
     * <p> The name of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithName(const Aws::String& value) { SetName(value); return *this;}

    /**
     * <p> The name of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}

    /**
     * <p> The name of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithName(const char* value) { SetName(value); return *this;}


    /**
     * <p> The value of the custom header. </p>
     */
    inline const Aws::String& GetValue() const{ return m_value; }

    /**
     * <p> The value of the custom header. </p>
     */
    inline bool ValueHasBeenSet() const { return m_valueHasBeenSet; }

    /**
     * <p> The value of the custom header. </p>
     */
    inline void SetValue(const Aws::String& value) { m_valueHasBeenSet = true; m_value = value; }

    /**
     * <p> The value of the custom header. </p>
     */
    inline void SetValue(Aws::String&& value) { m_valueHasBeenSet = true; m_value = std::move(value); }

    /**
     * <p> The value of the custom header. </p>
     */
    inline void SetValue(const char* value) { m_valueHasBeenSet = true; m_value.assign(value); }

    /**
     * <p> The value of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithValue(const Aws::String& value) { SetValue(value); return *this;}

    /**
     * <p> The value of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithValue(Aws::String&& value) { SetValue(std::move(value)); return *this;}

    /**
     * <p> The value of the custom header. </p>
     */
    inline AwsWafv2CustomHttpHeader& WithValue(const char* value) { SetValue(value); return *this;}

  private:

    Aws::String m_name;
    bool m_nameHasBeenSet = false;

    Aws::String m_value;
    bool m_valueHasBeenSet = false;
  };

} // namespace Model
} // namespace SecurityHub
} // namespace Aws