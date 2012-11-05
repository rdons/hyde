using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

namespace TechSmith.Hyde
{
   public class ConnectionStringCloudStorageAccount : ICloudStorageAccount
   {
      private readonly CloudStorageAccount _cloudStorageAccount;

      public ConnectionStringCloudStorageAccount( string connectionString )
      {
         // HACK: This is a workaround for a bug in the Azure 2.0 version of the SDK. Remove when it is fixed. Link: https://github.com/WindowsAzure/azure-sdk-for-net/pull/120
         if ( connectionString == "UseDevelopmentStorage=true" )
         {
            _cloudStorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
         }
         else
         {
            _cloudStorageAccount = CloudStorageAccount.Parse( connectionString );
         }
      }

      public string TableEndpoint
      {
         get
         {
            return _cloudStorageAccount.TableEndpoint.AbsoluteUri;
         }
      }

      public StorageCredentials Credentials
      {
         get
         {
            return _cloudStorageAccount.Credentials;
         }
      }
   }
}
