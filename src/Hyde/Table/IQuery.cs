using System.Collections.Generic;

namespace TechSmith.Hyde.Table
{
   public interface IFilterable<T> : IPkLowBoundedFilterable<T>
   {
      IBoundChoice<IPkLowBoundedFilterable<T>> PartitionKeyFrom( string value );
      IRkFilterable<T> PartitionKeyEquals( string value );
   }

   public interface IBoundChoice<T>
   {
      T Exclusive();
      T Inclusive();
   }

   public interface IPkLowBoundedFilterable<T> : IRkFilterable<T>
   {
      IBoundChoice<IRkFilterable<T>> PartitionKeyTo( string value );
   }

   public interface IRkFilterable<T> :IRkLowBoundedFilterable<T>
   {
      IBoundChoice<IRkLowBoundedFilterable<T>> RowKeyFrom( string value );
      IQuery<T> RowKeyEquals( string value );
   }

   public interface IRkLowBoundedFilterable<T> : IQuery<T>
   {
      IBoundChoice<IQuery<T>> RowKeyTo( string value );
   }

   public interface IQuery<T> : IEnumerable<T>
   {
      IEnumerable<T> Top( int count );
   }
}
