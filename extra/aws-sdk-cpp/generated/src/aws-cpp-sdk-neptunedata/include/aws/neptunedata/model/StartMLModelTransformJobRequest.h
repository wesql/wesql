﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/neptunedata/Neptunedata_EXPORTS.h>
#include <aws/neptunedata/NeptunedataRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/neptunedata/model/CustomModelTransformParameters.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <utility>

namespace Aws
{
namespace neptunedata
{
namespace Model
{

  /**
   */
  class StartMLModelTransformJobRequest : public NeptunedataRequest
  {
  public:
    AWS_NEPTUNEDATA_API StartMLModelTransformJobRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "StartMLModelTransformJob"; }

    AWS_NEPTUNEDATA_API Aws::String SerializePayload() const override;


    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline bool IdHasBeenSet() const { return m_idHasBeenSet; }

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline StartMLModelTransformJobRequest& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline StartMLModelTransformJobRequest& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * <p>A unique identifier for the new job. The default is an autogenerated
     * UUID.</p>
     */
    inline StartMLModelTransformJobRequest& WithId(const char* value) { SetId(value); return *this;}


    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline const Aws::String& GetDataProcessingJobId() const{ return m_dataProcessingJobId; }

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline bool DataProcessingJobIdHasBeenSet() const { return m_dataProcessingJobIdHasBeenSet; }

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetDataProcessingJobId(const Aws::String& value) { m_dataProcessingJobIdHasBeenSet = true; m_dataProcessingJobId = value; }

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetDataProcessingJobId(Aws::String&& value) { m_dataProcessingJobIdHasBeenSet = true; m_dataProcessingJobId = std::move(value); }

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetDataProcessingJobId(const char* value) { m_dataProcessingJobIdHasBeenSet = true; m_dataProcessingJobId.assign(value); }

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithDataProcessingJobId(const Aws::String& value) { SetDataProcessingJobId(value); return *this;}

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithDataProcessingJobId(Aws::String&& value) { SetDataProcessingJobId(std::move(value)); return *this;}

    /**
     * <p>The job ID of a completed data-processing job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithDataProcessingJobId(const char* value) { SetDataProcessingJobId(value); return *this;}


    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline const Aws::String& GetMlModelTrainingJobId() const{ return m_mlModelTrainingJobId; }

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline bool MlModelTrainingJobIdHasBeenSet() const { return m_mlModelTrainingJobIdHasBeenSet; }

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetMlModelTrainingJobId(const Aws::String& value) { m_mlModelTrainingJobIdHasBeenSet = true; m_mlModelTrainingJobId = value; }

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetMlModelTrainingJobId(Aws::String&& value) { m_mlModelTrainingJobIdHasBeenSet = true; m_mlModelTrainingJobId = std::move(value); }

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetMlModelTrainingJobId(const char* value) { m_mlModelTrainingJobIdHasBeenSet = true; m_mlModelTrainingJobId.assign(value); }

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithMlModelTrainingJobId(const Aws::String& value) { SetMlModelTrainingJobId(value); return *this;}

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithMlModelTrainingJobId(Aws::String&& value) { SetMlModelTrainingJobId(std::move(value)); return *this;}

    /**
     * <p>The job ID of a completed model-training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithMlModelTrainingJobId(const char* value) { SetMlModelTrainingJobId(value); return *this;}


    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline const Aws::String& GetTrainingJobName() const{ return m_trainingJobName; }

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline bool TrainingJobNameHasBeenSet() const { return m_trainingJobNameHasBeenSet; }

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetTrainingJobName(const Aws::String& value) { m_trainingJobNameHasBeenSet = true; m_trainingJobName = value; }

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetTrainingJobName(Aws::String&& value) { m_trainingJobNameHasBeenSet = true; m_trainingJobName = std::move(value); }

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline void SetTrainingJobName(const char* value) { m_trainingJobNameHasBeenSet = true; m_trainingJobName.assign(value); }

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithTrainingJobName(const Aws::String& value) { SetTrainingJobName(value); return *this;}

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithTrainingJobName(Aws::String&& value) { SetTrainingJobName(std::move(value)); return *this;}

    /**
     * <p>The name of a completed SageMaker training job. You must include either
     * <code>dataProcessingJobId</code> and a <code>mlModelTrainingJobId</code>, or a
     * <code>trainingJobName</code>.</p>
     */
    inline StartMLModelTransformJobRequest& WithTrainingJobName(const char* value) { SetTrainingJobName(value); return *this;}


    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline const Aws::String& GetModelTransformOutputS3Location() const{ return m_modelTransformOutputS3Location; }

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline bool ModelTransformOutputS3LocationHasBeenSet() const { return m_modelTransformOutputS3LocationHasBeenSet; }

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline void SetModelTransformOutputS3Location(const Aws::String& value) { m_modelTransformOutputS3LocationHasBeenSet = true; m_modelTransformOutputS3Location = value; }

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline void SetModelTransformOutputS3Location(Aws::String&& value) { m_modelTransformOutputS3LocationHasBeenSet = true; m_modelTransformOutputS3Location = std::move(value); }

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline void SetModelTransformOutputS3Location(const char* value) { m_modelTransformOutputS3LocationHasBeenSet = true; m_modelTransformOutputS3Location.assign(value); }

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline StartMLModelTransformJobRequest& WithModelTransformOutputS3Location(const Aws::String& value) { SetModelTransformOutputS3Location(value); return *this;}

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline StartMLModelTransformJobRequest& WithModelTransformOutputS3Location(Aws::String&& value) { SetModelTransformOutputS3Location(std::move(value)); return *this;}

    /**
     * <p>The location in Amazon S3 where the model artifacts are to be stored.</p>
     */
    inline StartMLModelTransformJobRequest& WithModelTransformOutputS3Location(const char* value) { SetModelTransformOutputS3Location(value); return *this;}


    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline const Aws::String& GetSagemakerIamRoleArn() const{ return m_sagemakerIamRoleArn; }

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline bool SagemakerIamRoleArnHasBeenSet() const { return m_sagemakerIamRoleArnHasBeenSet; }

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline void SetSagemakerIamRoleArn(const Aws::String& value) { m_sagemakerIamRoleArnHasBeenSet = true; m_sagemakerIamRoleArn = value; }

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline void SetSagemakerIamRoleArn(Aws::String&& value) { m_sagemakerIamRoleArnHasBeenSet = true; m_sagemakerIamRoleArn = std::move(value); }

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline void SetSagemakerIamRoleArn(const char* value) { m_sagemakerIamRoleArnHasBeenSet = true; m_sagemakerIamRoleArn.assign(value); }

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithSagemakerIamRoleArn(const Aws::String& value) { SetSagemakerIamRoleArn(value); return *this;}

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithSagemakerIamRoleArn(Aws::String&& value) { SetSagemakerIamRoleArn(std::move(value)); return *this;}

    /**
     * <p>The ARN of an IAM role for SageMaker execution. This must be listed in your
     * DB cluster parameter group or an error will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithSagemakerIamRoleArn(const char* value) { SetSagemakerIamRoleArn(value); return *this;}


    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline const Aws::String& GetNeptuneIamRoleArn() const{ return m_neptuneIamRoleArn; }

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline bool NeptuneIamRoleArnHasBeenSet() const { return m_neptuneIamRoleArnHasBeenSet; }

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline void SetNeptuneIamRoleArn(const Aws::String& value) { m_neptuneIamRoleArnHasBeenSet = true; m_neptuneIamRoleArn = value; }

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline void SetNeptuneIamRoleArn(Aws::String&& value) { m_neptuneIamRoleArnHasBeenSet = true; m_neptuneIamRoleArn = std::move(value); }

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline void SetNeptuneIamRoleArn(const char* value) { m_neptuneIamRoleArnHasBeenSet = true; m_neptuneIamRoleArn.assign(value); }

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithNeptuneIamRoleArn(const Aws::String& value) { SetNeptuneIamRoleArn(value); return *this;}

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithNeptuneIamRoleArn(Aws::String&& value) { SetNeptuneIamRoleArn(std::move(value)); return *this;}

    /**
     * <p>The ARN of an IAM role that provides Neptune access to SageMaker and Amazon
     * S3 resources. This must be listed in your DB cluster parameter group or an error
     * will occur.</p>
     */
    inline StartMLModelTransformJobRequest& WithNeptuneIamRoleArn(const char* value) { SetNeptuneIamRoleArn(value); return *this;}


    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline const CustomModelTransformParameters& GetCustomModelTransformParameters() const{ return m_customModelTransformParameters; }

    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline bool CustomModelTransformParametersHasBeenSet() const { return m_customModelTransformParametersHasBeenSet; }

    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline void SetCustomModelTransformParameters(const CustomModelTransformParameters& value) { m_customModelTransformParametersHasBeenSet = true; m_customModelTransformParameters = value; }

    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline void SetCustomModelTransformParameters(CustomModelTransformParameters&& value) { m_customModelTransformParametersHasBeenSet = true; m_customModelTransformParameters = std::move(value); }

    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline StartMLModelTransformJobRequest& WithCustomModelTransformParameters(const CustomModelTransformParameters& value) { SetCustomModelTransformParameters(value); return *this;}

    /**
     * <p>Configuration information for a model transform using a custom model. The
     * <code>customModelTransformParameters</code> object contains the following
     * fields, which must have values compatible with the saved model parameters from
     * the training job:</p>
     */
    inline StartMLModelTransformJobRequest& WithCustomModelTransformParameters(CustomModelTransformParameters&& value) { SetCustomModelTransformParameters(std::move(value)); return *this;}


    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline const Aws::String& GetBaseProcessingInstanceType() const{ return m_baseProcessingInstanceType; }

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline bool BaseProcessingInstanceTypeHasBeenSet() const { return m_baseProcessingInstanceTypeHasBeenSet; }

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline void SetBaseProcessingInstanceType(const Aws::String& value) { m_baseProcessingInstanceTypeHasBeenSet = true; m_baseProcessingInstanceType = value; }

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline void SetBaseProcessingInstanceType(Aws::String&& value) { m_baseProcessingInstanceTypeHasBeenSet = true; m_baseProcessingInstanceType = std::move(value); }

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline void SetBaseProcessingInstanceType(const char* value) { m_baseProcessingInstanceTypeHasBeenSet = true; m_baseProcessingInstanceType.assign(value); }

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline StartMLModelTransformJobRequest& WithBaseProcessingInstanceType(const Aws::String& value) { SetBaseProcessingInstanceType(value); return *this;}

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline StartMLModelTransformJobRequest& WithBaseProcessingInstanceType(Aws::String&& value) { SetBaseProcessingInstanceType(std::move(value)); return *this;}

    /**
     * <p>The type of ML instance used in preparing and managing training of ML models.
     * This is an ML compute instance chosen based on memory requirements for
     * processing the training data and model.</p>
     */
    inline StartMLModelTransformJobRequest& WithBaseProcessingInstanceType(const char* value) { SetBaseProcessingInstanceType(value); return *this;}


    /**
     * <p>The disk volume size of the training instance in gigabytes. The default is 0.
     * Both input data and the output model are stored on disk, so the volume size must
     * be large enough to hold both data sets. If not specified or 0, Neptune ML
     * selects a disk volume size based on the recommendation generated in the data
     * processing step.</p>
     */
    inline int GetBaseProcessingInstanceVolumeSizeInGB() const{ return m_baseProcessingInstanceVolumeSizeInGB; }

    /**
     * <p>The disk volume size of the training instance in gigabytes. The default is 0.
     * Both input data and the output model are stored on disk, so the volume size must
     * be large enough to hold both data sets. If not specified or 0, Neptune ML
     * selects a disk volume size based on the recommendation generated in the data
     * processing step.</p>
     */
    inline bool BaseProcessingInstanceVolumeSizeInGBHasBeenSet() const { return m_baseProcessingInstanceVolumeSizeInGBHasBeenSet; }

    /**
     * <p>The disk volume size of the training instance in gigabytes. The default is 0.
     * Both input data and the output model are stored on disk, so the volume size must
     * be large enough to hold both data sets. If not specified or 0, Neptune ML
     * selects a disk volume size based on the recommendation generated in the data
     * processing step.</p>
     */
    inline void SetBaseProcessingInstanceVolumeSizeInGB(int value) { m_baseProcessingInstanceVolumeSizeInGBHasBeenSet = true; m_baseProcessingInstanceVolumeSizeInGB = value; }

    /**
     * <p>The disk volume size of the training instance in gigabytes. The default is 0.
     * Both input data and the output model are stored on disk, so the volume size must
     * be large enough to hold both data sets. If not specified or 0, Neptune ML
     * selects a disk volume size based on the recommendation generated in the data
     * processing step.</p>
     */
    inline StartMLModelTransformJobRequest& WithBaseProcessingInstanceVolumeSizeInGB(int value) { SetBaseProcessingInstanceVolumeSizeInGB(value); return *this;}


    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline const Aws::Vector<Aws::String>& GetSubnets() const{ return m_subnets; }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline bool SubnetsHasBeenSet() const { return m_subnetsHasBeenSet; }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline void SetSubnets(const Aws::Vector<Aws::String>& value) { m_subnetsHasBeenSet = true; m_subnets = value; }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline void SetSubnets(Aws::Vector<Aws::String>&& value) { m_subnetsHasBeenSet = true; m_subnets = std::move(value); }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithSubnets(const Aws::Vector<Aws::String>& value) { SetSubnets(value); return *this;}

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithSubnets(Aws::Vector<Aws::String>&& value) { SetSubnets(std::move(value)); return *this;}

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSubnets(const Aws::String& value) { m_subnetsHasBeenSet = true; m_subnets.push_back(value); return *this; }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSubnets(Aws::String&& value) { m_subnetsHasBeenSet = true; m_subnets.push_back(std::move(value)); return *this; }

    /**
     * <p>The IDs of the subnets in the Neptune VPC. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSubnets(const char* value) { m_subnetsHasBeenSet = true; m_subnets.push_back(value); return *this; }


    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline const Aws::Vector<Aws::String>& GetSecurityGroupIds() const{ return m_securityGroupIds; }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline bool SecurityGroupIdsHasBeenSet() const { return m_securityGroupIdsHasBeenSet; }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline void SetSecurityGroupIds(const Aws::Vector<Aws::String>& value) { m_securityGroupIdsHasBeenSet = true; m_securityGroupIds = value; }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline void SetSecurityGroupIds(Aws::Vector<Aws::String>&& value) { m_securityGroupIdsHasBeenSet = true; m_securityGroupIds = std::move(value); }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithSecurityGroupIds(const Aws::Vector<Aws::String>& value) { SetSecurityGroupIds(value); return *this;}

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithSecurityGroupIds(Aws::Vector<Aws::String>&& value) { SetSecurityGroupIds(std::move(value)); return *this;}

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSecurityGroupIds(const Aws::String& value) { m_securityGroupIdsHasBeenSet = true; m_securityGroupIds.push_back(value); return *this; }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSecurityGroupIds(Aws::String&& value) { m_securityGroupIdsHasBeenSet = true; m_securityGroupIds.push_back(std::move(value)); return *this; }

    /**
     * <p>The VPC security group IDs. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& AddSecurityGroupIds(const char* value) { m_securityGroupIdsHasBeenSet = true; m_securityGroupIds.push_back(value); return *this; }


    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline const Aws::String& GetVolumeEncryptionKMSKey() const{ return m_volumeEncryptionKMSKey; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline bool VolumeEncryptionKMSKeyHasBeenSet() const { return m_volumeEncryptionKMSKeyHasBeenSet; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline void SetVolumeEncryptionKMSKey(const Aws::String& value) { m_volumeEncryptionKMSKeyHasBeenSet = true; m_volumeEncryptionKMSKey = value; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline void SetVolumeEncryptionKMSKey(Aws::String&& value) { m_volumeEncryptionKMSKeyHasBeenSet = true; m_volumeEncryptionKMSKey = std::move(value); }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline void SetVolumeEncryptionKMSKey(const char* value) { m_volumeEncryptionKMSKeyHasBeenSet = true; m_volumeEncryptionKMSKey.assign(value); }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithVolumeEncryptionKMSKey(const Aws::String& value) { SetVolumeEncryptionKMSKey(value); return *this;}

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithVolumeEncryptionKMSKey(Aws::String&& value) { SetVolumeEncryptionKMSKey(std::move(value)); return *this;}

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * data on the storage volume attached to the ML compute instances that run the
     * training job. The default is None.</p>
     */
    inline StartMLModelTransformJobRequest& WithVolumeEncryptionKMSKey(const char* value) { SetVolumeEncryptionKMSKey(value); return *this;}


    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline const Aws::String& GetS3OutputEncryptionKMSKey() const{ return m_s3OutputEncryptionKMSKey; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline bool S3OutputEncryptionKMSKeyHasBeenSet() const { return m_s3OutputEncryptionKMSKeyHasBeenSet; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline void SetS3OutputEncryptionKMSKey(const Aws::String& value) { m_s3OutputEncryptionKMSKeyHasBeenSet = true; m_s3OutputEncryptionKMSKey = value; }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline void SetS3OutputEncryptionKMSKey(Aws::String&& value) { m_s3OutputEncryptionKMSKeyHasBeenSet = true; m_s3OutputEncryptionKMSKey = std::move(value); }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline void SetS3OutputEncryptionKMSKey(const char* value) { m_s3OutputEncryptionKMSKeyHasBeenSet = true; m_s3OutputEncryptionKMSKey.assign(value); }

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline StartMLModelTransformJobRequest& WithS3OutputEncryptionKMSKey(const Aws::String& value) { SetS3OutputEncryptionKMSKey(value); return *this;}

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline StartMLModelTransformJobRequest& WithS3OutputEncryptionKMSKey(Aws::String&& value) { SetS3OutputEncryptionKMSKey(std::move(value)); return *this;}

    /**
     * <p>The Amazon Key Management Service (KMS) key that SageMaker uses to encrypt
     * the output of the processing job. The default is none.</p>
     */
    inline StartMLModelTransformJobRequest& WithS3OutputEncryptionKMSKey(const char* value) { SetS3OutputEncryptionKMSKey(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet = false;

    Aws::String m_dataProcessingJobId;
    bool m_dataProcessingJobIdHasBeenSet = false;

    Aws::String m_mlModelTrainingJobId;
    bool m_mlModelTrainingJobIdHasBeenSet = false;

    Aws::String m_trainingJobName;
    bool m_trainingJobNameHasBeenSet = false;

    Aws::String m_modelTransformOutputS3Location;
    bool m_modelTransformOutputS3LocationHasBeenSet = false;

    Aws::String m_sagemakerIamRoleArn;
    bool m_sagemakerIamRoleArnHasBeenSet = false;

    Aws::String m_neptuneIamRoleArn;
    bool m_neptuneIamRoleArnHasBeenSet = false;

    CustomModelTransformParameters m_customModelTransformParameters;
    bool m_customModelTransformParametersHasBeenSet = false;

    Aws::String m_baseProcessingInstanceType;
    bool m_baseProcessingInstanceTypeHasBeenSet = false;

    int m_baseProcessingInstanceVolumeSizeInGB;
    bool m_baseProcessingInstanceVolumeSizeInGBHasBeenSet = false;

    Aws::Vector<Aws::String> m_subnets;
    bool m_subnetsHasBeenSet = false;

    Aws::Vector<Aws::String> m_securityGroupIds;
    bool m_securityGroupIdsHasBeenSet = false;

    Aws::String m_volumeEncryptionKMSKey;
    bool m_volumeEncryptionKMSKeyHasBeenSet = false;

    Aws::String m_s3OutputEncryptionKMSKey;
    bool m_s3OutputEncryptionKMSKeyHasBeenSet = false;
  };

} // namespace Model
} // namespace neptunedata
} // namespace Aws