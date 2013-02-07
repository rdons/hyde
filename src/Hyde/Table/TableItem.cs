using System;
using System.Collections.Generic;
using System.Linq;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Common.DataAnnotations;

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

      public static TableItem Create( object entity, bool throwOnReservedPropertyName )
      {
         var item = CreateFromType( entity, throwOnReservedPropertyName );

         if ( item.PartitionKey == null )
         {
            throw new ArgumentException( "Missing PartitionKey property" );
         }

         if ( item.RowKey == null )
         {
            throw new ArgumentException( "Missing RowKey property" );
         }

         return item;
      }

      public static TableItem Create( object entity, string partitionKey, string rowKey, bool throwOnReservedPropertyName )
      {
         var item = CreateFromType( entity, throwOnReservedPropertyName );

         if ( item.PartitionKey != null && item.PartitionKey != partitionKey )
         {
            throw new ArgumentException( string.Format( "Entity defines PartitionKey: {0} but it conflicts with partitionKey argument: {1}", item.PartitionKey, partitionKey ) );
         }
         item.PartitionKey = partitionKey;

         if ( item.RowKey != null && item.RowKey != rowKey )
         {
            throw new ArgumentException( string.Format( "Entity defines RowKey: {0} but it conflicts with rowKey argument: {1}", item.RowKey, rowKey ) );
         }
         item.RowKey = rowKey;

         return item;
      }

      private static TableItem CreateFromType( object entity, bool throwOnReservedPropertyName )
      {
         var properties = new Dictionary<string, Tuple<object, Type>>();
         foreach ( var property in entity.GetType().GetProperties().Where( p => p.ShouldSerialize() ) )
         {
            properties[property.Name] = new Tuple<object, Type>( property.GetValue( entity, null ), property.PropertyType );
         }

         var item = new TableItem( properties, throwOnReservedPropertyName );

         if ( entity.HasPropertyDecoratedWith<PartitionKeyAttribute>() )
         {
            item.PartitionKey = entity.ReadPropertyDecoratedWith<PartitionKeyAttribute, string>();
         }

         if ( entity.HasPropertyDecoratedWith<RowKeyAttribute>() )
         {
            item.RowKey = entity.ReadPropertyDecoratedWith<RowKeyAttribute, string>();
         }
         return item;
      }
   }
}