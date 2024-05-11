﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/rekognition/Rekognition_EXPORTS.h>
#include <aws/rekognition/model/VideoJobStatus.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/rekognition/model/VideoMetadata.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/rekognition/model/Video.h>
#include <aws/rekognition/model/PersonMatch.h>
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
namespace Rekognition
{
namespace Model
{
  class GetFaceSearchResult
  {
  public:
    AWS_REKOGNITION_API GetFaceSearchResult();
    AWS_REKOGNITION_API GetFaceSearchResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_REKOGNITION_API GetFaceSearchResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The current status of the face search job.</p>
     */
    inline const VideoJobStatus& GetJobStatus() const{ return m_jobStatus; }

    /**
     * <p>The current status of the face search job.</p>
     */
    inline void SetJobStatus(const VideoJobStatus& value) { m_jobStatus = value; }

    /**
     * <p>The current status of the face search job.</p>
     */
    inline void SetJobStatus(VideoJobStatus&& value) { m_jobStatus = std::move(value); }

    /**
     * <p>The current status of the face search job.</p>
     */
    inline GetFaceSearchResult& WithJobStatus(const VideoJobStatus& value) { SetJobStatus(value); return *this;}

    /**
     * <p>The current status of the face search job.</p>
     */
    inline GetFaceSearchResult& WithJobStatus(VideoJobStatus&& value) { SetJobStatus(std::move(value)); return *this;}


    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline const Aws::String& GetStatusMessage() const{ return m_statusMessage; }

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline void SetStatusMessage(const Aws::String& value) { m_statusMessage = value; }

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline void SetStatusMessage(Aws::String&& value) { m_statusMessage = std::move(value); }

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline void SetStatusMessage(const char* value) { m_statusMessage.assign(value); }

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline GetFaceSearchResult& WithStatusMessage(const Aws::String& value) { SetStatusMessage(value); return *this;}

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline GetFaceSearchResult& WithStatusMessage(Aws::String&& value) { SetStatusMessage(std::move(value)); return *this;}

    /**
     * <p>If the job fails, <code>StatusMessage</code> provides a descriptive error
     * message.</p>
     */
    inline GetFaceSearchResult& WithStatusMessage(const char* value) { SetStatusMessage(value); return *this;}


    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextToken = value; }

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextToken = std::move(value); }

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline void SetNextToken(const char* value) { m_nextToken.assign(value); }

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline GetFaceSearchResult& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline GetFaceSearchResult& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>If the response is truncated, Amazon Rekognition Video returns this token
     * that you can use in the subsequent request to retrieve the next set of search
     * results. </p>
     */
    inline GetFaceSearchResult& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    /**
     * <p>Information about a video that Amazon Rekognition analyzed.
     * <code>Videometadata</code> is returned in every page of paginated responses from
     * a Amazon Rekognition Video operation. </p>
     */
    inline const VideoMetadata& GetVideoMetadata() const{ return m_videoMetadata; }

    /**
     * <p>Information about a video that Amazon Rekognition analyzed.
     * <code>Videometadata</code> is returned in every page of paginated responses from
     * a Amazon Rekognition Video operation. </p>
     */
    inline void SetVideoMetadata(const VideoMetadata& value) { m_videoMetadata = value; }

    /**
     * <p>Information about a video that Amazon Rekognition analyzed.
     * <code>Videometadata</code> is returned in every page of paginated responses from
     * a Amazon Rekognition Video operation. </p>
     */
    inline void SetVideoMetadata(VideoMetadata&& value) { m_videoMetadata = std::move(value); }

    /**
     * <p>Information about a video that Amazon Rekognition analyzed.
     * <code>Videometadata</code> is returned in every page of paginated responses from
     * a Amazon Rekognition Video operation. </p>
     */
    inline GetFaceSearchResult& WithVideoMetadata(const VideoMetadata& value) { SetVideoMetadata(value); return *this;}

    /**
     * <p>Information about a video that Amazon Rekognition analyzed.
     * <code>Videometadata</code> is returned in every page of paginated responses from
     * a Amazon Rekognition Video operation. </p>
     */
    inline GetFaceSearchResult& WithVideoMetadata(VideoMetadata&& value) { SetVideoMetadata(std::move(value)); return *this;}


    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline const Aws::Vector<PersonMatch>& GetPersons() const{ return m_persons; }

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline void SetPersons(const Aws::Vector<PersonMatch>& value) { m_persons = value; }

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline void SetPersons(Aws::Vector<PersonMatch>&& value) { m_persons = std::move(value); }

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline GetFaceSearchResult& WithPersons(const Aws::Vector<PersonMatch>& value) { SetPersons(value); return *this;}

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline GetFaceSearchResult& WithPersons(Aws::Vector<PersonMatch>&& value) { SetPersons(std::move(value)); return *this;}

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline GetFaceSearchResult& AddPersons(const PersonMatch& value) { m_persons.push_back(value); return *this; }

    /**
     * <p>An array of persons, <a>PersonMatch</a>, in the video whose face(s) match the
     * face(s) in an Amazon Rekognition collection. It also includes time information
     * for when persons are matched in the video. You specify the input collection in
     * an initial call to <code>StartFaceSearch</code>. Each <code>Persons</code>
     * element includes a time the person was matched, face match details
     * (<code>FaceMatches</code>) for matching faces in the collection, and person
     * information (<code>Person</code>) for the matched person. </p>
     */
    inline GetFaceSearchResult& AddPersons(PersonMatch&& value) { m_persons.push_back(std::move(value)); return *this; }


    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline const Aws::String& GetJobId() const{ return m_jobId; }

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline void SetJobId(const Aws::String& value) { m_jobId = value; }

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline void SetJobId(Aws::String&& value) { m_jobId = std::move(value); }

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline void SetJobId(const char* value) { m_jobId.assign(value); }

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline GetFaceSearchResult& WithJobId(const Aws::String& value) { SetJobId(value); return *this;}

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline GetFaceSearchResult& WithJobId(Aws::String&& value) { SetJobId(std::move(value)); return *this;}

    /**
     * <p>Job identifier for the face search operation for which you want to obtain
     * results. The job identifer is returned by an initial call to
     * StartFaceSearch.</p>
     */
    inline GetFaceSearchResult& WithJobId(const char* value) { SetJobId(value); return *this;}


    
    inline const Video& GetVideo() const{ return m_video; }

    
    inline void SetVideo(const Video& value) { m_video = value; }

    
    inline void SetVideo(Video&& value) { m_video = std::move(value); }

    
    inline GetFaceSearchResult& WithVideo(const Video& value) { SetVideo(value); return *this;}

    
    inline GetFaceSearchResult& WithVideo(Video&& value) { SetVideo(std::move(value)); return *this;}


    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline const Aws::String& GetJobTag() const{ return m_jobTag; }

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline void SetJobTag(const Aws::String& value) { m_jobTag = value; }

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline void SetJobTag(Aws::String&& value) { m_jobTag = std::move(value); }

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline void SetJobTag(const char* value) { m_jobTag.assign(value); }

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline GetFaceSearchResult& WithJobTag(const Aws::String& value) { SetJobTag(value); return *this;}

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline GetFaceSearchResult& WithJobTag(Aws::String&& value) { SetJobTag(std::move(value)); return *this;}

    /**
     * <p>A job identifier specified in the call to StartFaceSearch and returned in the
     * job completion notification sent to your Amazon Simple Notification Service
     * topic.</p>
     */
    inline GetFaceSearchResult& WithJobTag(const char* value) { SetJobTag(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline GetFaceSearchResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline GetFaceSearchResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline GetFaceSearchResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    VideoJobStatus m_jobStatus;

    Aws::String m_statusMessage;

    Aws::String m_nextToken;

    VideoMetadata m_videoMetadata;

    Aws::Vector<PersonMatch> m_persons;

    Aws::String m_jobId;

    Video m_video;

    Aws::String m_jobTag;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace Rekognition
} // namespace Aws