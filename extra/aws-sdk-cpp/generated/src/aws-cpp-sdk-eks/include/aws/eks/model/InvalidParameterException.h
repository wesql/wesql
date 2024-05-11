﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/eks/EKS_EXPORTS.h>
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
namespace EKS
{
namespace Model
{

  /**
   * <p>The specified parameter is invalid. Review the available parameters for the
   * API request.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/eks-2017-11-01/InvalidParameterException">AWS
   * API Reference</a></p>
   */
  class InvalidParameterException
  {
  public:
    AWS_EKS_API InvalidParameterException();
    AWS_EKS_API InvalidParameterException(Aws::Utils::Json::JsonView jsonValue);
    AWS_EKS_API InvalidParameterException& operator=(Aws::Utils::Json::JsonView jsonValue);
    AWS_EKS_API Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline const Aws::String& GetClusterName() const{ return m_clusterName; }

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline bool ClusterNameHasBeenSet() const { return m_clusterNameHasBeenSet; }

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline void SetClusterName(const Aws::String& value) { m_clusterNameHasBeenSet = true; m_clusterName = value; }

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline void SetClusterName(Aws::String&& value) { m_clusterNameHasBeenSet = true; m_clusterName = std::move(value); }

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline void SetClusterName(const char* value) { m_clusterNameHasBeenSet = true; m_clusterName.assign(value); }

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline InvalidParameterException& WithClusterName(const Aws::String& value) { SetClusterName(value); return *this;}

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline InvalidParameterException& WithClusterName(Aws::String&& value) { SetClusterName(std::move(value)); return *this;}

    /**
     * <p>The Amazon EKS cluster associated with the exception.</p>
     */
    inline InvalidParameterException& WithClusterName(const char* value) { SetClusterName(value); return *this;}


    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline const Aws::String& GetNodegroupName() const{ return m_nodegroupName; }

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline bool NodegroupNameHasBeenSet() const { return m_nodegroupNameHasBeenSet; }

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline void SetNodegroupName(const Aws::String& value) { m_nodegroupNameHasBeenSet = true; m_nodegroupName = value; }

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline void SetNodegroupName(Aws::String&& value) { m_nodegroupNameHasBeenSet = true; m_nodegroupName = std::move(value); }

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline void SetNodegroupName(const char* value) { m_nodegroupNameHasBeenSet = true; m_nodegroupName.assign(value); }

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline InvalidParameterException& WithNodegroupName(const Aws::String& value) { SetNodegroupName(value); return *this;}

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline InvalidParameterException& WithNodegroupName(Aws::String&& value) { SetNodegroupName(std::move(value)); return *this;}

    /**
     * <p>The Amazon EKS managed node group associated with the exception.</p>
     */
    inline InvalidParameterException& WithNodegroupName(const char* value) { SetNodegroupName(value); return *this;}


    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline const Aws::String& GetFargateProfileName() const{ return m_fargateProfileName; }

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline bool FargateProfileNameHasBeenSet() const { return m_fargateProfileNameHasBeenSet; }

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline void SetFargateProfileName(const Aws::String& value) { m_fargateProfileNameHasBeenSet = true; m_fargateProfileName = value; }

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline void SetFargateProfileName(Aws::String&& value) { m_fargateProfileNameHasBeenSet = true; m_fargateProfileName = std::move(value); }

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline void SetFargateProfileName(const char* value) { m_fargateProfileNameHasBeenSet = true; m_fargateProfileName.assign(value); }

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline InvalidParameterException& WithFargateProfileName(const Aws::String& value) { SetFargateProfileName(value); return *this;}

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline InvalidParameterException& WithFargateProfileName(Aws::String&& value) { SetFargateProfileName(std::move(value)); return *this;}

    /**
     * <p>The Fargate profile associated with the exception.</p>
     */
    inline InvalidParameterException& WithFargateProfileName(const char* value) { SetFargateProfileName(value); return *this;}


    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline const Aws::String& GetAddonName() const{ return m_addonName; }

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline bool AddonNameHasBeenSet() const { return m_addonNameHasBeenSet; }

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline void SetAddonName(const Aws::String& value) { m_addonNameHasBeenSet = true; m_addonName = value; }

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline void SetAddonName(Aws::String&& value) { m_addonNameHasBeenSet = true; m_addonName = std::move(value); }

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline void SetAddonName(const char* value) { m_addonNameHasBeenSet = true; m_addonName.assign(value); }

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline InvalidParameterException& WithAddonName(const Aws::String& value) { SetAddonName(value); return *this;}

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline InvalidParameterException& WithAddonName(Aws::String&& value) { SetAddonName(std::move(value)); return *this;}

    /**
     * <p>The specified parameter for the add-on name is invalid. Review the available
     * parameters for the API request</p>
     */
    inline InvalidParameterException& WithAddonName(const char* value) { SetAddonName(value); return *this;}


    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline const Aws::String& GetSubscriptionId() const{ return m_subscriptionId; }

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline bool SubscriptionIdHasBeenSet() const { return m_subscriptionIdHasBeenSet; }

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline void SetSubscriptionId(const Aws::String& value) { m_subscriptionIdHasBeenSet = true; m_subscriptionId = value; }

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline void SetSubscriptionId(Aws::String&& value) { m_subscriptionIdHasBeenSet = true; m_subscriptionId = std::move(value); }

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline void SetSubscriptionId(const char* value) { m_subscriptionIdHasBeenSet = true; m_subscriptionId.assign(value); }

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline InvalidParameterException& WithSubscriptionId(const Aws::String& value) { SetSubscriptionId(value); return *this;}

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline InvalidParameterException& WithSubscriptionId(Aws::String&& value) { SetSubscriptionId(std::move(value)); return *this;}

    /**
     * <p>The Amazon EKS subscription ID with the exception.</p>
     */
    inline InvalidParameterException& WithSubscriptionId(const char* value) { SetSubscriptionId(value); return *this;}


    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline const Aws::String& GetMessage() const{ return m_message; }

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline bool MessageHasBeenSet() const { return m_messageHasBeenSet; }

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline void SetMessage(const Aws::String& value) { m_messageHasBeenSet = true; m_message = value; }

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline void SetMessage(Aws::String&& value) { m_messageHasBeenSet = true; m_message = std::move(value); }

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline void SetMessage(const char* value) { m_messageHasBeenSet = true; m_message.assign(value); }

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline InvalidParameterException& WithMessage(const Aws::String& value) { SetMessage(value); return *this;}

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline InvalidParameterException& WithMessage(Aws::String&& value) { SetMessage(std::move(value)); return *this;}

    /**
     * <p>The specified parameter is invalid. Review the available parameters for the
     * API request.</p>
     */
    inline InvalidParameterException& WithMessage(const char* value) { SetMessage(value); return *this;}

  private:

    Aws::String m_clusterName;
    bool m_clusterNameHasBeenSet = false;

    Aws::String m_nodegroupName;
    bool m_nodegroupNameHasBeenSet = false;

    Aws::String m_fargateProfileName;
    bool m_fargateProfileNameHasBeenSet = false;

    Aws::String m_addonName;
    bool m_addonNameHasBeenSet = false;

    Aws::String m_subscriptionId;
    bool m_subscriptionIdHasBeenSet = false;

    Aws::String m_message;
    bool m_messageHasBeenSet = false;
  };

} // namespace Model
} // namespace EKS
} // namespace Aws