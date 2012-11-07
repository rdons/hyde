using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Common.DataAnnotations;

namespace TechSmith.Hyde.Table.Azure
{
   internal class GenericTableEntity : ITableEntity
   {
      private IDictionary<string, EntityProperty> _properties;

      private static readonly HashSet<string> _invalidPropertyNames = new HashSet<string>
                                                                     {
                                                                        "PartitionKey",
                                                                        "RowKey",
                                                                     };

      private static readonly Dictionary<Type, Func<EntityProperty, object>> _typeToConverterFunction = new Dictionary<Type, Func<EntityProperty, object>>
        {
           { typeof( string ), p => p.StringValue },
           { typeof( int ), p => p.Int32Value },
           { typeof( int? ), p => p.StringValue == null ? (int?) null : p.Int32Value },
           { typeof( double ), p => p.DoubleValue },
           { typeof( double? ), p => p.StringValue == null ? (double?) null : p.DoubleValue },
           { typeof( byte[] ), p => p.BinaryValue },
           { typeof( Guid ), p => p.GuidValue },
           { typeof( Guid? ), p => p.StringValue == null ? (Guid?) null : p.GuidValue },
           { typeof( DateTime ), p => p.DateTimeOffsetValue.Value.UtcDateTime },
           { typeof( DateTime? ), p => p.StringValue == null ? (DateTime?) null : p.DateTimeOffsetValue.Value.UtcDateTime },
           { typeof( bool ), p => p.BooleanValue },
           { typeof( bool? ), p => p.StringValue == null ? (bool?) null : p.BooleanValue },
           { typeof( long ), p => p.Int64Value },
           { typeof( long? ), p => p.StringValue == null ? (long?) null : p.Int64Value },
           { typeof( Uri ), p => p.StringValue == null ? null : new Uri( p.StringValue ) },
        };

      private static readonly Dictionary<EdmType, Func<EntityProperty, object>> _edmTypeToConverterFunction = new Dictionary<EdmType, Func<EntityProperty, object>>
        {
           { EdmType.Binary, p => p.BinaryValue },
           { EdmType.Boolean, p => p.StringValue == null ? (bool?) null : p.BooleanValue },
           { EdmType.DateTime, p => p.DateTimeOffsetValue.HasValue ? p.DateTimeOffsetValue.Value.UtcDateTime : (DateTime?) null  },
           { EdmType.Double, p => p.StringValue == null ? (double?) null : p.DoubleValue },
           { EdmType.Guid, p => p.StringValue == null ? (Guid?) null : p.GuidValue },
           { EdmType.Int32, p => p.StringValue == null ? (int?) null : p.Int32Value },
           { EdmType.Int64, p => p.StringValue == null ? (long?) null : p.Int64Value },
           { EdmType.String, p => p.StringValue },
        };

      public string PartitionKey
      {
         get;
         set;
      }

      public string RowKey
      {
         get;
         set;
      }

      public DateTimeOffset Timestamp
      {
         get;
         set;
      }

      public string ETag
      {
         get;
         set;
      }

      public void ReadEntity( IDictionary<string, EntityProperty> properties, OperationContext operationContext )
      {
         _properties = properties;
      }

      public IDictionary<string, EntityProperty> WriteEntity( OperationContext operationContext )
      {
         throw new NotImplementedException();
      }

      //public static GenericTableEntity HydrateFrom( dynamic sourceItem, string partitionKey, string rowKey )
      //{
      //   Dictionary<string, EntityPropertyInfo> properties = GetProperties( sourceItem );

      //   var genericEntity = new GenericTableEntity();
      //   if ( properties != null )
      //   {
      //      foreach ( KeyValuePair<string, EntityPropertyInfo> keyValuePair in properties )
      //      {
      //         if ( _invalidPropertyNames.Contains( keyValuePair.Key ) )
      //         {
      //            throw new InvalidEntityException( string.Format( "Invalid property name {0}", keyValuePair.Key ) );
      //         }
      //         genericEntity[keyValuePair.Key] = keyValuePair.Value;
      //      }
      //   }

      //   genericEntity.PartitionKey = partitionKey;
      //   genericEntity.RowKey = rowKey;

      //   return genericEntity;
      //}

      //private static Dictionary<string, EntityPropertyInfo> GetProperties( dynamic item )
      //{
      //   if ( item is IDynamicMetaObjectProvider )
      //   {
      //      return GetPropertiesFromDynamicMetaObject( item );
      //   }
      //   return GetPropertiesFromType( item );
      //}

      //private static Dictionary<string, EntityPropertyInfo> GetPropertiesFromDynamicMetaObject( IDynamicMetaObjectProvider item )
      //{
      //   var properties = new Dictionary<string, EntityPropertyInfo>();
      //   IEnumerable<string> memberNames = ImpromptuInterface.Impromptu.GetMemberNames( item );
      //   foreach ( var memberName in memberNames )
      //   {
      //      dynamic result = ImpromptuInterface.Impromptu.InvokeGet( item, memberName );
      //      Type objectType = result == null ? typeof( object ) : result.GetType();
      //      properties[memberName] = new EntityPropertyInfo( result, objectType, result == null );
      //   }
      //   return properties;
      //}

      //private static Dictionary<string, EntityPropertyInfo> GetPropertiesFromType<T>( T item )
      //{
      //   var properties = new Dictionary<string, EntityPropertyInfo>();
      //   foreach ( var property in item.GetType().GetProperties().Where( p => p.ShouldSerialize() ) )
      //   {
      //      if ( _invalidPropertyNames.Contains( property.Name ) )
      //      {
      //         throw new InvalidEntityException( string.Format( "Invalid property name {0}", property.Name ) );
      //      }
      //      var valueOfProperty = property.GetValue( item, null );
      //      properties[property.Name] = new EntityPropertyInfo( valueOfProperty, property.PropertyType, valueOfProperty == null );
      //   }
      //   return properties;
      //}

      public dynamic ConvertToDynamic()
      {
         dynamic newItem = new ExpandoObject();

         if ( _properties == null )
         {
            return newItem;
         }

         foreach ( KeyValuePair<string, EntityProperty> property in _properties )
         {
            Func<EntityProperty, object> func = _edmTypeToConverterFunction[property.Value.PropertyType];
            ( (IDictionary<string, object>) newItem ).Add( property.Key, func( property.Value ) );
         }

         return newItem;
      }

      public T ConvertTo<T>() where T : new()
      {
         Type objectType = typeof( T );

         if ( _properties == null )
         {
            return (T) ( objectType.IsValueType ? Activator.CreateInstance( objectType ) : null );
         }

         var newItem = new T();

         SetRowKeyAndPartitionKeyOnObject( newItem );

         objectType.GetProperties().Where( p => p.ShouldSerialize() )
                                   .Where( p => _properties.ContainsKey( p.Name ) )
                                   .ToList()
                                   .ForEach( p => SetPropertyOnObject( p, newItem ) );

         return newItem;
      }

      private void SetPropertyOnObject<T>( PropertyInfo typeProperty, T newItem ) where T : new()
      {
         if ( typeProperty.Name == "PartitionKey" || typeProperty.Name == "RowKey" || typeProperty.Name == "Timestamp" || typeProperty.Name == "ETag" )
         {
            return;
         }

         Func<EntityProperty, object> funcToCall = _typeToConverterFunction[typeProperty.PropertyType];
         object propertyValue = funcToCall( _properties[typeProperty.Name] );

         typeProperty.SetValue( newItem, propertyValue, null );
      }

      private void SetRowKeyAndPartitionKeyOnObject<T>( T newItem ) where T : new()
      {
         var partitionKeyProperty = newItem.FindPropertyDecoratedWith<PartitionKeyAttribute>();
         if ( partitionKeyProperty != null )
         {
            partitionKeyProperty.SetValue( newItem, PartitionKey, null );
         }

         var rowKeyProperty = newItem.FindPropertyDecoratedWith<RowKeyAttribute>();
         if ( rowKeyProperty != null )
         {
            rowKeyProperty.SetValue( newItem, RowKey, null );
         }
      }
   }
}