using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Storage.Blob.SubContainers
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var storageConnectionString = GetConnString();

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            string subContainer = "Images";
            string subSubContainer = "Party";

            if (CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
            {
                try
                {
                    // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
                    CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                    cloudBlobContainer = cloudBlobClient.GetContainerReference("quick-blobcontainer");
                    if (await cloudBlobContainer.CreateIfNotExistsAsync())
                    {
                        Console.WriteLine("Created container '{0}'", cloudBlobContainer.Name);
                        Console.WriteLine();
                    }


                    // Upload #1
                    var blobName = $"{subContainer}/{Guid.NewGuid()}";
                    Console.WriteLine("Uploading to Blob storage as blob '{0}'", blobName);
                    Console.WriteLine();

                    CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
                    await cloudBlockBlob.UploadTextAsync("Hello, world!");

                    // Upload #2
                    blobName = $"{subContainer}/{subSubContainer}/{Guid.NewGuid()}";
                    Console.WriteLine("Uploading to Blob storage as blob '{0}'", blobName);
                    Console.WriteLine();

                    cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
                    await cloudBlockBlob.UploadTextAsync("Hello, world!");
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Error returned from the service: {0}", ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Unexpected exception: {0}", ex.Message);
                }
                finally
                {
                    Console.WriteLine("Press any key to delete the sample blobs and example container.");
                    Console.ReadLine();
                    // Clean up resources. This includes the container and the two temp files.
                    Console.WriteLine("Deleting the container and any blobs it contains");
                    if (cloudBlobContainer != null)
                    {
                        await cloudBlobContainer.DeleteIfExistsAsync();
                    }
                }
            }
        }

        static string GetConnString()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();

            return configuration["Azure.Storage.ConnectionString"];
        }
    }
}
