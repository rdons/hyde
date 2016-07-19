using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.Test
{
   [TestClass]
   public class HashRepositoryTests
   {
      [TestMethod]
      public async Task GetHash_HashToRetrieveIsInRepository_ExpectHashIsReturned()
      {
         InMemoryTableStorageProvider.ResetAllTables();
         var hashRepository = new HashRepository( new InMemoryTableStorageProvider() );
         hashRepository.AddHash( new Hash
         {
            Url = "a"
         } );
         await hashRepository.SaveChangesAsync();

         var result = await hashRepository.GetHash( "a" );

         Assert.AreEqual( "a", result.Url );
      }
   }

   public class Hash
   {
      public string Url
      {
         get;
         set;
      }
   }

   public class HashRepository
   {
      readonly string _hashPartitionKey = "hashPartition";
      private readonly TableStorageProvider _tableStorageProvider;

      public HashRepository( TableStorageProvider tableStorageProvider )
      {
         _tableStorageProvider = tableStorageProvider;
      }

      public void AddHash( Hash hashToAdd )
      {
         _tableStorageProvider.Add( string.Empty, hashToAdd, _hashPartitionKey, hashToAdd.Url );
      }

      public Task<Hash> GetHash( string hashUrl )
      {
         return _tableStorageProvider.GetAsync<Hash>( string.Empty, _hashPartitionKey, hashUrl );
      }

      public Task SaveChangesAsync()
      {
         return _tableStorageProvider.SaveAsync();
      }
   }
}