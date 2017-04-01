using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Xml;

namespace AzureStorageRestClient
{
    class Program
    {
        static string _protocol;
        static string _accountName;
        static string _accountKey;
        static string _shareName;
        static bool _verbose = false;
        const int TimeOutSecs = 100;

        static void Main(string[] args)
        {
            try
            {
                _protocol = "https";
                _shareName = string.Empty;
                _accountName = string.Empty;
                _accountKey = string.Empty;
                _verbose = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        static async Task DeleteDirectory(string path, bool recursive)
        {
            if (recursive)
            {
                var results = await ListDirectories(path);
                foreach (var dir in results.Item1.Entries.Select(d => $"{path}/{d.Name}"))
                {
                    await DeleteDirectory(dir, recursive);
                }

                foreach (var file in results.Item2.Entries.Select(f => $"{path}/{f.Name}"))
                {
                    await DeleteFile(file);
                }
            }

            await DeleteDirectory(path);
        }

        static async Task<(EnumerationResults<DirectoryResult>, EnumerationResults<FileResult>)> ListDirectories(string path)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?restype=directory&comp=list&timeout={TimeOutSecs}");

            using (var response = await HttpInvoke(HttpMethod.Get, uri))
            {
                response.EnsureSuccessStatusCode();

                var xml = await response.Content.ReadAsStringAsync();

                var serializer = new DataContractSerializer(typeof(EnumerationResults<DirectoryResult>));
                EnumerationResults<DirectoryResult> dirResult;
                using (var stringReader = new StringReader(xml))
                {
                    using (var xmlReader = XmlReader.Create(stringReader))
                    {
                        dirResult = (EnumerationResults<DirectoryResult>)serializer.ReadObject(xmlReader);
                    }
                }

                serializer = new DataContractSerializer(typeof(EnumerationResults<FileResult>));
                EnumerationResults<FileResult> fileResult;
                using (var stringReader = new StringReader(xml))
                {
                    using (var xmlReader = XmlReader.Create(stringReader))
                    {
                        fileResult = (EnumerationResults<FileResult>)serializer.ReadObject(xmlReader);
                    }
                }
                return (dirResult, fileResult);
            }
        }

        static async Task GetFileProperties(string path) 
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?timeout={TimeOutSecs}");

            using (var response = await HttpInvoke(HttpMethod.Head, uri))
            {
                response.EnsureSuccessStatusCode();
            }
        }

        static async Task<bool> FileContentMatches(string path, string file)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?timeout={TimeOutSecs}");

            uint hash1;
            uint hash2;
            using (var response = await HttpInvoke(HttpMethod.Get, uri))
            {
                response.EnsureSuccessStatusCode();

                using (var source = await response.Content.ReadAsStreamAsync())
                {
                    hash1 = Fnv1aHashHelper.ComputeHash(source, (int)response.Content.Headers.ContentLength.Value);
                }
            }

            var info = new FileInfo(file);
            using (var stream = info.OpenRead())
            {
                hash2 = Fnv1aHashHelper.ComputeHash(stream, (int)info.Length);
            }

            return hash1 == hash2;
        }

        static async Task DownloadFile(string path, string destFile)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?timeout={TimeOutSecs}");

            using (var response = await HttpInvoke(HttpMethod.Get, uri))
            {
                response.EnsureSuccessStatusCode();

                using (var source = await response.Content.ReadAsStreamAsync())
                {
                    using (var dest = File.Open(destFile, FileMode.Create))
                    {
                        await source.CopyToAsync(dest);
                    }
                }

                File.SetLastWriteTimeUtc(destFile, response.Content.Headers.LastModified.Value.DateTime);
            }
        }

        static async Task UploadFile(string path, string srcFile)
        {
            var file = new FileInfo(srcFile);

            await EnsureDirectories(path);

            await EnsureFile(path, file.Length);

            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?comp=range&timeout={TimeOutSecs}");

            // Max upload range
            var buffer = new byte[4 * 1_024 * 1_024];
            var total = 0;
            using (var stream = file.OpenRead())
            {
                while (total < file.Length)
                {
                    var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (read == 0)
                    {
                        break;
                    }

                    var content = new ByteArrayContent(buffer, 0, read);
                    var handler = new Action<HttpRequestMessage>(request =>
                    {
                        request.Headers.Add("x-ms-range", $"bytes={total}-{total + read - 1}");
                        request.Headers.Add("x-ms-write", "update");
                        request.Content = content;
                    });

                    using (var response = await HttpInvoke(HttpMethod.Put, uri, handler))
                    {
                        response.EnsureSuccessStatusCode();
                    }

                    total += read;
                }
            }
        }

        static async Task DeleteDirectory(string path)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?restype=directory&timeout={TimeOutSecs}");
            using (var response = await HttpInvoke(HttpMethod.Delete, uri))
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return;
                }

                response.EnsureSuccessStatusCode();
            }
        }

        static async Task DeleteFile(string path)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?timeout={TimeOutSecs}");
            using (var response = await HttpInvoke(HttpMethod.Delete, uri))
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return;
                }

                response.EnsureSuccessStatusCode();
            }
        }

        static async Task EnsureFile(string path, long length)
        {
            var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{path.Trim('/')}?timeout={TimeOutSecs}");

            await DeleteFile(path);

            var handler = new Action<HttpRequestMessage>(request =>
            {
                // This header specifies the maximum size for the file, up to 256MB.
                request.Headers.Add("x-ms-content-length", $"{length}");
                request.Headers.Add("x-ms-type", "file");
            });

            using (var response = await HttpInvoke(HttpMethod.Put, uri, handler))
            {
                response.EnsureSuccessStatusCode();
            }
        }

        static async Task EnsureDirectories(string path)
        {
            var paths = path.Trim('/').Split('/');
            for (int i = 0; i < paths.Length - 1; i++)
            {
                var dirPath = string.Join("/", paths.Take(i + 1));

                var uri = new Uri($"{_protocol}://{_accountName}.file.core.windows.net/{_shareName}/{dirPath.Trim('/')}?restype=directory&timeout={TimeOutSecs}");

                using (var response = await HttpInvoke(HttpMethod.Get, uri))
                {
                    if (response.IsSuccessStatusCode)
                    {
                        continue;
                    }
                }

                using (var response = await HttpInvoke(HttpMethod.Put, uri))
                {
                    response.EnsureSuccessStatusCode();
                }
            }
        }


        static async Task<HttpResponseMessage> HttpInvoke(HttpMethod method, Uri uri, Action<HttpRequestMessage> handler = null)
        {
            var requestId = Guid.NewGuid().ToString();
            var dateInRfc1123Format = DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture);
            using (var client = new HttpClient())
            {
                var request = new HttpRequestMessage(method, uri);
                request.Headers.Add("x-ms-request-id", requestId);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/xml"));
                request.Headers.Add("x-ms-date", dateInRfc1123Format);
                request.Headers.Add("x-ms-version", "2015-02-21");

                if (handler != null)
                {
                    handler(request);
                }

                client.DefaultRequestHeaders.Authorization = CreateAuthorizationHeader(_accountName, _accountKey, request);

                TraceLine();
                TraceLine("{0} {1}", request.Method, request.RequestUri.PathAndQuery);
                foreach (var header in request.Headers)
                {
                    TraceLine("{0}: {1}", header.Key, string.Join("; ", header.Value));
                }

                if (request.Content != null)
                {
                    foreach (var header in request.Content.Headers)
                    {
                        TraceLine("{0}: {1}", header.Key, string.Join("; ", header.Value));
                    }
                }

                var response = await client.SendAsync(request);

                TraceLine();
                TraceLine("HTTP /1.1 {0} {1}", (int)response.StatusCode, (HttpStatusCode)response.StatusCode);
                foreach (var header in response.Headers)
                {
                    TraceLine("{0}: {1}", header.Key, string.Join("; ", header.Value));
                }
                foreach (var header in response.Content.Headers)
                {
                    TraceLine("{0}: {1}", header.Key, string.Join("; ", header.Value));
                }

                return response;
            }
        }

        private static AuthenticationHeaderValue CreateAuthorizationHeader(string accountName, string accountKey, HttpRequestMessage request)
        {
            //StringToSign = VERB + "\n" +  
            //               Content-Encoding + "\n" +  
            //               Content-Language + "\n" +  
            //               Content-Length + "\n" +  
            //               Content-MD5 + "\n" +  
            //               Content-Type + "\n" +  
            //               Date + "\n" +  
            //               If-Modified-Since + "\n" +  
            //               If-Match + "\n" +  
            //               If-None-Match + "\n" +  
            //               If-Unmodified-Since + "\n" +  
            //               Range + "\n" +  
            //               CanonicalizedHeaders +   
            //               CanonicalizedResource;  

            var pathOnly = request.RequestUri.AbsolutePath;
            var query = string.Join("\n", request.RequestUri.Query.Split('?')[1]
                .Split(new[] { '&' })
                .Select(n => n.Split('='))
                .Select(a => string.Join(":", a[0].ToLowerInvariant(), a[1]))
                .OrderBy(n => n));
            var headerValues = string.Join("\n", request.Headers
                .Where(h => h.Key.StartsWith("x-ms-"))
                .Select(h => string.Join(":", h.Key.ToLowerInvariant(), string.Join(";", h.Value)))
                .OrderBy(n => n));

            string canonicalizedString = null;
            if (request.Content != null)
            {
                canonicalizedString = string.Format("{0}\n\n\n{1}\n\n\n\n\n\n\n\n\n{2}\n{3}\n{4}",
                    request.Method,
                    request.Content.Headers.ContentLength,
                    headerValues,
                    "/" + accountName + pathOnly,
                    query);
            }
            else
            {
                canonicalizedString = string.Format("{0}\n\n\n\n\n\n\n\n\n\n\n\n{1}\n{2}\n{3}",
                    request.Method,
                    headerValues,
                    "/" + accountName + pathOnly,
                    query);
            }

            string signature = String.Empty;
            using (HMACSHA256 hmacSha256 = new HMACSHA256(Convert.FromBase64String(accountKey)))
            {
                Byte[] dataToHmac = System.Text.Encoding.UTF8.GetBytes(canonicalizedString);
                signature = Convert.ToBase64String(hmacSha256.ComputeHash(dataToHmac));
            }

            return new AuthenticationHeaderValue("SharedKey", string.Format("{0}:{1}", accountName, signature));
        }

        private static void TraceLine()
        {
            if (_verbose)
            {
                Console.WriteLine();
            }
        }

        private static void TraceLine(string message)
        {
            if (_verbose)
            {
                TraceLine("{0}", message);
            }
        }

        private static void TraceLine(string format, params object[] args)
        {
            if (_verbose)
            {
                Console.WriteLine(format, args);
            }
        }
    }
}
