using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using TechSmith.Hyde.Table.Azure;

namespace TechSmith.Hyde.Table.Memory
{
   internal class DynamicMemoryQuery : AbstractMemoryQuery<dynamic>
   {
      public DynamicMemoryQuery( IEnumerable<GenericTableEntity> entities, bool shouldIncludeETagForDynamic )
         : base( entities )
      {
         IncludeETagForDynamic = shouldIncludeETagForDynamic;
      }

      private DynamicMemoryQuery( DynamicMemoryQuery previous, bool shouldIncludeETagForDynamic )
         : base( previous )
      {
         IncludeETagForDynamic = shouldIncludeETagForDynamic;
      }

      protected override AbstractQuery<dynamic> CreateCopy()
      {
         return new DynamicMemoryQuery( this, IncludeETagForDynamic );
      }

      internal override dynamic Convert( GenericTableEntity e )
      {
         return StripNullValues( e.ConvertToDynamic( IncludeETagForDynamic ) );
      }

      private static dynamic StripNullValues( dynamic obj )
      {
         dynamic result = new ExpandoObject();
         foreach ( var p in ( obj as IDictionary<string, object> ).Where( p => p.Value != null ) )
         {
            ( (IDictionary<string, object>) result ).Add( p );
         }
         return result;
      }
   }
}
