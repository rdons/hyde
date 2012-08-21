using Microsoft.WindowsAzure;

namespace TechSmith.Hyde
{
   public interface ICloudStorageAccount
   {
      string TableEndpoint
      {
         get;
      }

      StorageCredentials Credentials
      {
         get;
      }
   }
}