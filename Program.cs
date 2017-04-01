using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace AzureStorageRestClient
{
    class Program
    {
        static string _protocol;
        static string _accountName;
        static string _accountKey;
        static string _shareName;
        static bool _verbose = false;

        static void Main(string[] args)
        {
            try
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("Usage: AzureStorageRestClient.exe StorageConnectionString FileShareName");
                    return;
                }

                var connectionString = args[0];
                var properties = connectionString
                    .Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(p => p.Split(new[] { '=' }, 2))
                    .ToDictionary(p => p[0], p => p[1]);

                _shareName = args[1];
                _protocol = properties["DefaultEndpointsProtocol"];
                _accountName = properties["AccountName"];
                _accountKey = properties["AccountKey"];
                _verbose = args.Length > 2;

                var etags = GetFunctionJsonLastModifiedTimes().Result;
                foreach (var pair in etags)
                {
                    Console.WriteLine("{0} {1}", pair.Value, pair.Key);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        static async Task<Dictionary<string, DateTimeOffset?>> GetFunctionJsonLastModifiedTimes()
        {
            var folders = await ListDirectories();
            TraceLine(string.Join(",", folders.Entries.Select(e => e.Name)));

            var tasks = new List<Task<Tuple<string, DateTimeOffset?>>>();
            tasks.AddRange(folders.Entries.Select(e => GetFunctionJsonLastModifiedTime(e.Name)));

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception)
            {
                // only care about success ones
            }

            return tasks
                .Where(t => !t.IsFaulted && !t.IsCanceled)
                .ToDictionary(t => t.Result.Item1, t => t.Result.Item2);
        }

        static async Task<Tuple<string, DateTimeOffset?>> GetFunctionJsonLastModifiedTime(string functionName)
        {
            var uri = new Uri(string.Format(
                "{0}://{1}.file.core.windows.net/{2}/site/wwwroot/{3}/function.json?timeout=30",
                _protocol, _accountName, _shareName, functionName));

            using (var response = await HttpInvoke(HttpMethod.Head, uri))
            {
                return new Tuple<string, DateTimeOffset?>(functionName, response.Content.Headers.LastModified);    
            }
        }

        static async Task<EnumerationResults<DirectoryResult>> ListDirectories()
        {
            var uri = new Uri(string.Format(
                "{0}://{1}.file.core.windows.net/{2}/site/wwwroot?restype=directory&comp=list&timeout=30",
                _protocol, _accountName, _shareName));

            using (var response = await HttpInvoke(HttpMethod.Get, uri))
            {
                var serializer = new DataContractSerializer(typeof(EnumerationResults<DirectoryResult>));
                return (EnumerationResults<DirectoryResult>)serializer.ReadObject(await response.Content.ReadAsStreamAsync());
            }
        }

        static async Task<HttpResponseMessage> HttpInvoke(HttpMethod method, Uri uri)
        {
            var requestId = Guid.NewGuid().ToString();
            var dateInRfc1123Format = DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture);
            using (var client = new HttpClient())
            {
                var request = new HttpRequestMessage(method, uri);

                client.DefaultRequestHeaders.Add("x-ms-request-id", requestId);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/xml"));
                client.DefaultRequestHeaders.Add("x-ms-date", dateInRfc1123Format);
                client.DefaultRequestHeaders.Add("x-ms-version", "2015-02-21");

                client.DefaultRequestHeaders.Authorization = CreateAuthorizationHeader(_accountName, _accountKey, request, client.DefaultRequestHeaders);

                TraceLine();
                TraceLine("{0} {1}", request.Method, request.RequestUri.PathAndQuery);
                foreach (var header in client.DefaultRequestHeaders)
                {
                    TraceLine("{0}: {1}", header.Key, string.Join("; ", header.Value));
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

                return response.EnsureSuccessStatusCode();
            }
        }

        private static AuthenticationHeaderValue CreateAuthorizationHeader(string accountName, string accountKey, HttpRequestMessage request, HttpRequestHeaders headers)
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
            var headerValues = string.Join("\n", headers
                .Where(h => h.Key.StartsWith("x-ms-"))
                .Select(h => string.Join(":", h.Key.ToLowerInvariant(), string.Join(";", h.Value)))
                .OrderBy(n => n));

            string canonicalizedString = string.Format("{0}\n\n\n\n\n\n\n\n\n\n\n\n{1}\n{2}\n{3}",
                request.Method,
                headerValues,
                "/" + accountName + pathOnly,
                query);

            //Console.WriteLine("====");
            //Console.WriteLine(canonicalizedString);
            //Console.WriteLine("====");

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
