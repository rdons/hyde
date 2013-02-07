using System;
using System.Collections.Generic;
using System.Linq;
using TechSmith.Hyde.Common;

namespace TechSmith.Hyde.Table
{
   public class TableItem
   {
      public string PartitionKey
      {
         get;
         private set;
      }

      public string RowKey
      {
         get;
         private set;
      }

      public Dictionary<string, Tuple<object, Type>> Properties
      {
         get;
         private set;
      }

      private TableItem( Dictionary<string, Tuple<object, Type>> properties, bool throwOnReservedProperties )
      {
         Properties = new Dictionary<string, Tuple<object, Type>>();
         foreach ( string propertyName in properties.Keys )
         {
            if ( !TableConstants.ReservedPropertyNames.Contains( propertyName ) )
            {
               Properties[propertyName] = properties[propertyName];
            }
            else if ( throwOnReservedProperties )
            {
               throw new InvalidEntityException( string.Format( "Reserved property name {0}", propertyName ) );
            }
         }
      }

      public static TableItem Create( object entity, string partitionKey, string rowKey, bool throwOnReservedPropertyName )
      {
         var properties = new Dictionary<string, Tuple<object, Type>>();
         foreach ( var property in entity.GetType().GetProperties().Where( p => p.ShouldSerialize() ) )
         {
            properties[property.Name] = new Tuple<object, Type>( property.GetValue( entity, null ), property.PropertyType );
         }
         var item = new TableItem( properties, throwOnReservedPropertyName );
         item.PartitionKey = partitionKey;
         item.RowKey = rowKey;
         return item;
      }
   }
}