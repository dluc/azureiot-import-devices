using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace _Console
{
    class Program
    {
        private const string IOT_HUB_CONN_STRING = "HostName=_____.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=_____";
        private const string STORAGE_ACCOUNT_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=_____;AccountKey=_____;EndpointSuffix=core.windows.net";

        private static RegistryManager registry;

        static void Main(string[] args)
        {
            Console.WriteLine("=== START ===");

            RunAsync().Wait();

            Console.WriteLine("=== END ===");
        }

        static async Task RunAsync()
        {
            registry = RegistryManager.CreateFromConnectionString(IOT_HUB_CONN_STRING);

            Console.WriteLine("Creating job 1...");
            var jobId1 = await CreateDevicesAsync(new[] { "test1", "test2" });

            string jobId2 = null;
            while (jobId2 == null)
            {
                var job = await registry.GetJobAsync(jobId1);
                Console.WriteLine("Job 1 status: " + job.Status);

                Console.WriteLine("Creating job 2...");
                jobId2 = await CreateDevicesAsync(new[] { "test3", "test4" });

                Thread.Sleep(2000);
            }
        }

        static async Task<string> CreateDevicesAsync(IList<string> devices)
        {
            try
            {
                var jobId = await BulkCreateListAsync(devices);
                Console.WriteLine("Job created: " + jobId);
                return jobId;
            }
            catch (Exception e)
            {
                Console.WriteLine("Job creation ERROR: " + e.GetType().FullName + ": " + e.Message);
            }

            return null;
        }

        static async Task<string> BulkCreateListAsync(IList<string> deviceIds)
        {
            var serializedDevices = deviceIds
                .Select(id => new ExportImportDevice { Id = id, ImportMode = ImportMode.CreateOrUpdate })
                .Select(JsonConvert.SerializeObject);

            CloudBlockBlob blob = await WriteDevicesToBlobAsync(serializedDevices);
            string containerUri = blob.Container.StorageUri.PrimaryUri.AbsoluteUri + GetSasToken();
            JobProperties job = await registry.ImportDevicesAsync(containerUri, containerUri, blob.Name);

            return job.JobId;
        }

        static async Task<CloudBlockBlob> WriteDevicesToBlobAsync(IEnumerable<string> devices)
        {
            var sb = new StringBuilder();
            devices.ToList().ForEach(device => sb.AppendLine(device));

            CloudBlockBlob blob = await CreateBlobAsync();
            using (var stream = await blob.OpenWriteAsync())
            {
                byte[] bytes = Encoding.UTF8.GetBytes(sb.ToString());
                for (var i = 0; i < bytes.Length; i += 500)
                {
                    int length = Math.Min(bytes.Length - i, 500);
                    await stream.WriteAsync(bytes, i, length);
                }
            }

            return blob;
        }

        static async Task<CloudBlockBlob> CreateBlobAsync()
        {
            string containerName = ("iothub-" + DateTimeOffset.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss-") + Guid.NewGuid().ToString("N")).ToLowerInvariant();
            string blobName = "devices.txt";

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(STORAGE_ACCOUNT_CONN_STRING);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            await container.CreateIfNotExistsAsync();

            return blob;
        }

        static string GetSasToken()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(STORAGE_ACCOUNT_CONN_STRING);
            var policy = new SharedAccessAccountPolicy
            {
                Permissions = SharedAccessAccountPermissions.Read | SharedAccessAccountPermissions.Write | SharedAccessAccountPermissions.Delete,
                Services = SharedAccessAccountServices.Blob,
                ResourceTypes = SharedAccessAccountResourceTypes.Container | SharedAccessAccountResourceTypes.Object,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(60),
                Protocols = SharedAccessProtocol.HttpsOnly
            };

            return storageAccount.GetSharedAccessSignature(policy);
        }
    }
}
