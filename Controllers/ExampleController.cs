using System.IO.Compression;
using System.Net;
using System.Web;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using Microsoft.AspNetCore.Mvc;

namespace amazon_s3_service_example.Controllers;

[ApiController]
[Route("example")]
public class ExampleController : ControllerBase
{
    private readonly AmazonS3Client _amazonS3Client;

    private readonly string _bucketName = "wonjun-s3";
    
    public ExampleController(IConfiguration configuration)
    {
        AmazonS3Config config = new AmazonS3Config();

        config.RegionEndpoint = RegionEndpoint.GetBySystemName(configuration["Storage:S3:Region"]);

        _amazonS3Client = new AmazonS3Client(configuration["Storage:S3:AccessKey"], configuration["Storage:S3:SecretAccessKey"], config);   
    }

    // bucket 특정 directory 객체 목록 가져오기
    [HttpGet("search/dicrectory/{path}")]
    public async Task<ActionResult> GetDicrectoryObjectInfo(string path)
    {
        try
        {
            ListObjectsRequest request = new ListObjectsRequest() {
                BucketName = _bucketName,
                Prefix = HttpUtility.UrlDecode(path)
            };

            ListObjectsResponse response = await _amazonS3Client.ListObjectsAsync(request);

            if (response.HttpStatusCode == HttpStatusCode.OK) return Ok(response.S3Objects.Select(x => x.Key).ToList());
            else return NoContent();
        }

        catch (AmazonS3Exception)
        {
            return NotFound();
        }

        catch (Exception)
        {
            return StatusCode(500);
        }
    }

    // 객체 정보 가져오기
    [HttpGet("search/object/{key}")]
    public async Task<ActionResult> GetObjectInfo(string key)
    {
        try
        {
            GetObjectRequest request = new GetObjectRequest() {
                BucketName = _bucketName,
                Key = HttpUtility.UrlDecode(key)
            };

            using (GetObjectResponse response = await _amazonS3Client.GetObjectAsync(request)) 
            {
                if (response.HttpStatusCode == HttpStatusCode.OK) return Ok(response.Key);
                else return NoContent();
            }
        }

        catch (AmazonS3Exception)
        {
            return NotFound();
        }

        catch (Exception)
        {
            return StatusCode(500);
        }
    }

    [HttpGet("presigendUrl/{key}")]
    public ActionResult GetDownloadPresignedUrl(string key)
    {
        GetPreSignedUrlRequest request = new GetPreSignedUrlRequest() {
            BucketName = _bucketName,
            Key = HttpUtility.UrlDecode(key),
            Verb = HttpVerb.GET, // default GET
            Expires = DateTime.UtcNow.AddSeconds(10)
        };

        string presignedUrl = _amazonS3Client.GetPreSignedURL(request);

        return Ok(presignedUrl);
    }

    [HttpPost("presigendUrl")]
    public ActionResult GetUploadPresignedUrl([FromForm] string key)
    {
        GetPreSignedUrlRequest request = new GetPreSignedUrlRequest() {
            BucketName = _bucketName,
            Key = HttpUtility.UrlDecode(key),
            Verb = HttpVerb.PUT,
            Expires = DateTime.UtcNow.AddSeconds(10)
        };

        string presignedUrl = _amazonS3Client.GetPreSignedURL(request);

        return Ok(presignedUrl);
    }

    // multipart upload
    [HttpPost("upload/multipart")]
    public async Task<ActionResult> MultipartUpload([FromForm] string? directoryPath, IFormFileCollection files)
    {
        try
        {
            #region 병렬처리
            // files.AsParallel().ForAll(async file => {
            //     string key = string.IsNullOrEmpty(directoryPath) ? file.FileName : string.Format("{0}/{1}", directoryPath, file.FileName);

            //     using (MemoryStream ms = new MemoryStream()) 
            //     {
            //         await file.OpenReadStream().CopyToAsync(ms);

            //         List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();

            //         // MultipartUpload 설정 초기화
            //         InitiateMultipartUploadResponse initResponse = await _amazonS3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest {
            //             BucketName = _bucketName,
            //             Key = key
            //         });

            //         // Part의 최소 크기는 5MB
            //         long contentLength = ms.Length;
            //         long partSize = 5 * (long)Math.Pow(2, 20);

            //         try
            //         {
            //             long filePosition = 0;

            //             // 파일을 chunk로 쪼개서 요청 생성
            //             for (int i = 1; filePosition < contentLength; i++)
            //             {
            //                 UploadPartRequest uploadRequest = new UploadPartRequest {
            //                     BucketName = _bucketName,
            //                     Key = key,
            //                     UploadId = initResponse.UploadId,
            //                     PartNumber = i,
            //                     PartSize = partSize,
            //                     FilePosition = filePosition,
            //                     InputStream = ms
            //                 };

            //                 uploadResponses.Add(await _amazonS3Client.UploadPartAsync(uploadRequest));

            //                 filePosition += partSize;
            //             }

            //             // 완료 요청 설정
            //             CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest {
            //                 BucketName = _bucketName,
            //                 Key = key,
            //                 UploadId = initResponse.UploadId
            //             };

            //             // 각 part의 ETAG 
            //             completeRequest.AddPartETags(uploadResponses);

            //             // 완료 요청을 보내 Upload 마무리
            //             // 완료 요청을 보내지 않으면 파일이 Upload 되지 않음
            //             CompleteMultipartUploadResponse completeUploadResponse = await _amazonS3Client.CompleteMultipartUploadAsync(completeRequest);
            //         }

            //         catch (Exception)
            //         {
            //             // 오류 발생 시 Upload 중단
            //             // 해당 UploadId 폐기
            //             AbortMultipartUploadRequest abortMPURequest = new AbortMultipartUploadRequest
            //             {
            //                 BucketName = _bucketName,
            //                 Key = key,
            //                 UploadId = initResponse.UploadId
            //             };

            //             await _amazonS3Client.AbortMultipartUploadAsync(abortMPURequest);
            //         }
            //     }
            // });
            #endregion

            foreach(IFormFile file in files) 
            {
                string key = string.IsNullOrEmpty(directoryPath) ? file.FileName : string.Format("{0}/{1}", directoryPath, file.FileName);

                List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();

                // MultipartUpload 설정 초기화
                InitiateMultipartUploadResponse initResponse = await _amazonS3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest {
                    BucketName = _bucketName,
                    Key = key,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
                });

                Stream fileStream = file.OpenReadStream();

                // Part의 최소 크기는 5MB
                long contentLength = fileStream.Length;
                long partSize = 5 * (long)Math.Pow(2, 20);

                try
                {
                    long filePosition = 0;

                    // 파일을 chunk로 쪼개서 요청 생성
                    for (int i = 1; filePosition < contentLength; i++)
                    {
                        UploadPartRequest uploadRequest = new UploadPartRequest {
                            BucketName = _bucketName,
                            Key = key,
                            UploadId = initResponse.UploadId,
                            PartNumber = i,
                            PartSize = partSize,
                            FilePosition = filePosition,
                            InputStream = fileStream
                        };

                        uploadResponses.Add(await _amazonS3Client.UploadPartAsync(uploadRequest));

                        filePosition += partSize;
                    }

                    // 완료 요청 설정
                    CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest {
                        BucketName = _bucketName,
                        Key = key,
                        UploadId = initResponse.UploadId
                    };

                    // 각 part의 ETAG 
                    completeRequest.AddPartETags(uploadResponses);

                    // 완료 요청을 보내 Upload 마무리
                    // 완료 요청을 보내지 않으면 파일이 Upload 되지 않음
                    CompleteMultipartUploadResponse completeUploadResponse = await _amazonS3Client.CompleteMultipartUploadAsync(completeRequest);
                }

                catch (Exception)
                {
                    // 오류 발생 시 Upload 중단
                    // 해당 UploadId 폐기
                    AbortMultipartUploadRequest abortMPURequest = new AbortMultipartUploadRequest
                    {
                        BucketName = _bucketName,
                        Key = key,
                        UploadId = initResponse.UploadId
                    };

                    await _amazonS3Client.AbortMultipartUploadAsync(abortMPURequest);
                }
            }

            return Ok();
        }

        catch (Exception)
        {
            return StatusCode(500);
        }
    }

    // upload
    [HttpPost("upload")]
    public async Task<ActionResult> Upload([FromForm] string? directoryPath, IFormFileCollection files)
    {
        try
        {
            foreach(IFormFile file in files) 
            {
                string key = string.IsNullOrEmpty(directoryPath) ? file.FileName : string.Format("{0}/{1}", directoryPath, file.FileName);

                using (MemoryStream ms = new MemoryStream())
                {
                    await file.CopyToAsync(ms);

                    // 일반 Upload
                    var obj = new PutObjectRequest {
                        BucketName = _bucketName,
                        Key = key,
                        InputStream = ms,
                        ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
                    };
                    
                    PutObjectResponse putObjectResponse = await _amazonS3Client.PutObjectAsync(obj);
                }
            }

            return Ok();
        }

        catch (Exception)
        {
            return StatusCode(500);
        }
    }

    // bucket 특정 directory 객체 다운로드
    [HttpGet("download/dicrectory/{path}")]
    public async Task<ActionResult> DownloadDirectory(string path)
    {
        ListObjectsRequest request = new ListObjectsRequest() {
            BucketName = _bucketName,   
            Prefix = HttpUtility.UrlDecode(path)
        };

        ListObjectsResponse response = await _amazonS3Client.ListObjectsAsync(request);

        if (response.HttpStatusCode == HttpStatusCode.OK) 
        {
            using (MemoryStream ms = new MemoryStream())  
            {  
                using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, true))  
                {
                    foreach (S3Object obj in response.S3Objects) 
                    {
                        GetObjectRequest requestObject = new GetObjectRequest() {
                            BucketName = _bucketName,
                            Key = obj.Key
                        };

                        using (GetObjectResponse responseObject = await _amazonS3Client.GetObjectAsync(requestObject)) 
                        {
                            var entry = zip.CreateEntry(responseObject.Key.Split('/').LastOrDefault()!);

                            // using (var entryStream = entry.Open())
                            // using (BrotliStream bs = new BrotliStream(entryStream, CompressionLevel.NoCompression))
                            // {
                            //     await responseObject.ResponseStream.CopyToAsync(bs);
                            // }

                            using (var entryStream = entry.Open())
                            {
                                await responseObject.ResponseStream.CopyToAsync(entryStream);
                            }
                        }
                    }
                }

                return new FileContentResult(ms.ToArray(), "application/octet-stream") { FileDownloadName = "file.zip", EnableRangeProcessing = true };
            }
        }

        else 
            return NoContent();
    }

    // bucket object 다운로드
    [HttpGet("download/object/{key}")]
    public async Task<ActionResult> DownloadObject(string key)
    {
        try
        {
            GetObjectRequest request = new GetObjectRequest() {
                BucketName = _bucketName,
                Key = HttpUtility.UrlDecode(key)
            };

            using (GetObjectResponse response = await _amazonS3Client.GetObjectAsync(request)) 
            {
                if (response.HttpStatusCode == HttpStatusCode.OK) 
                {
                    using(MemoryStream ms = new MemoryStream())
                    {
                        await response.ResponseStream.CopyToAsync(ms);

                        return new FileContentResult(ms.ToArray(), "application/octet-stream") { FileDownloadName = response.Key.Split('/').LastOrDefault(), EnableRangeProcessing = true };
                    }
                }

                else 
                    return NoContent();
            }
        }

        catch (AmazonS3Exception)
        {
            return NotFound();
        }

        catch (Exception)
        {
            return StatusCode(500);
        }
    }


    [HttpDelete("delete/directory/{path}")]
    public async Task<ActionResult> DeleteDirectory(string path)
    {
        ListObjectsRequest request = new ListObjectsRequest() {
            BucketName = _bucketName,
            Prefix = HttpUtility.UrlDecode(path)
        };

        ListObjectsResponse response = await _amazonS3Client.ListObjectsAsync(request);

        if (response.HttpStatusCode == HttpStatusCode.OK) 
        {
            response.S3Objects.ForEach(async obj => await _amazonS3Client.DeleteObjectAsync(_bucketName, obj.Key));

            return NoContent();
        }

        else 
        {
            return NotFound();
        }
    }

    // bucket object 삭제
    [HttpDelete("delete/object/{key}")]
    public async Task<ActionResult> DeleteObject(string key)
    {   
        try
        {
            GetObjectRequest request = new GetObjectRequest() {
                BucketName = _bucketName,
                Key = HttpUtility.UrlDecode(key)
            };

            using (GetObjectResponse response = await _amazonS3Client.GetObjectAsync(request)) 
            {
                if (response.HttpStatusCode == HttpStatusCode.OK) 
                {
                    DeleteObjectRequest deleteRequest = new DeleteObjectRequest {
                        BucketName = _bucketName,
                        Key = HttpUtility.UrlDecode(key)
                    };

                    DeleteObjectResponse deleteResponse = await _amazonS3Client.DeleteObjectAsync(deleteRequest);

                    if (deleteResponse.HttpStatusCode == HttpStatusCode.NoContent) return NoContent();
                    else return NotFound();
                }

                else return NotFound();
            }
        }

        catch (AmazonS3Exception)
        {
            return NotFound();
        }

        catch (Exception) 
        {
            return StatusCode(500);
        }
    }
}