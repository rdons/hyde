using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.Common
{
   public static class IPartialResultTaskExtensions
   {
      /// <summary>
      /// Converts a partial result into a single enumerable by eagerly fetching more results. This may result in results being
      /// fetched and loaded into memory faster than they are being processed.
      /// </summary>
      /// <typeparam name="T">The type of the result being returned.</typeparam>
      /// <param name="asyncResult">The result being flattened.</param>
      /// <returns></returns>
      public static Task<IEnumerable<T>> FlattenAsync<T>( this Task<IPartialResult<T>> asyncResult )
      {
         var overallCompletionSource = new TaskCompletionSource<IEnumerable<T>>();
         IEnumerable<T> resultSet = Enumerable.Empty<T>();
         MergeAndContinueIfNecessary( asyncResult, resultSet, overallCompletionSource );
         return overallCompletionSource.Task;
      }

      private static void MergeAndContinueIfNecessary<T>( Task<IPartialResult<T>> asyncResult, IEnumerable<T> aggregator, TaskCompletionSource<IEnumerable<T>> overallCompletionSource )
      {
         asyncResult.ContinueWith( antecedent =>
         {
            try
            {
               aggregator = aggregator.Concat( antecedent.Result );
               var partialResult = antecedent.Result;
               if ( partialResult.HasMoreResults && !QueryHasBeenSatisfied( aggregator, partialResult.Query ) )
               {
                  MergeAndContinueIfNecessary<T>( antecedent.Result.GetNextAsync(), aggregator, overallCompletionSource );
               }
               else
               {
                  overallCompletionSource.SetResult( aggregator );
               }
            }
            catch ( Exception e )
            {
               // Catch synchronous exceptions in our continuation and propagate them.
               overallCompletionSource.SetException( e );
            }
         }, TaskContinuationOptions.OnlyOnRanToCompletion );
         asyncResult.ContinueWith( ( Task faultedTask ) => faultedTask.Exception.Handle( overallCompletionSource.TrySetException ), TaskContinuationOptions.OnlyOnFaulted );
      }

      private static bool QueryHasBeenSatisfied<T>( IEnumerable<T> aggregator, QueryDescriptor query )
      {
         return query.TopCount.HasValue && aggregator.Count() >= query.TopCount.Value;
      }
   }
}
