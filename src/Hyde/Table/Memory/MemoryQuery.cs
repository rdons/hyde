using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace TechSmith.Hyde.Table.Memory
{
   internal class MemoryQuery<T> : IQuery<T> where T : new()
   {
      private IEnumerable<T> _results;

      public MemoryQuery( IEnumerable<T> results )
      {
         _results = results;
      }

      public IQuery<T> Take( int count )
      {
         return new MemoryQuery<T>( _results.Take( count ) );
      }

      public IEnumerator<T> GetEnumerator()
      {
         return _results.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }
}
