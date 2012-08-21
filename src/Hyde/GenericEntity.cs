using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace TechSmith.Hyde
{
   // Modified from Jai Haridas: http://social.msdn.microsoft.com/Forums/en-US/windowsazure/thread/481afa1b-03a9-42d9-ae79-9d5dc33b9297/
   internal class GenericEntity : Microsoft.WindowsAzure.StorageClient.TableServiceEntity
   {
      private static readonly HashSet<string> InvalidPropertyNames = new HashSet<string>
                                                                     {
                                                                        "PartitionKey",
                                                                        "RowKey",
                                                                     };

      private readonly Dictionary<string, EntityPropertyInfo> _properties = new Dictionary<string, EntityPropertyInfo>();

      private static readonly Dictionary<Type, Func<string, object>> _typeToConverterFunction = new Dictionary<Type, Func<string, object>>();

      internal EntityPropertyInfo this[string key]
      {
         get
         {
            return _properties.ContainsKey( key ) ? _properties[key] : new EntityPropertyInfo( null, typeof( object ), true );
         }

         set
         {
            _properties[key] = value;
         }
      }

      public Dictionary<string, EntityPropertyInfo> GetProperties()
      {
         return _properties;
      }

      public override string ToString()
      {
         return string.Empty;
      }

      static GenericEntity()
      {
         _typeToConverterFunction.Add( typeof( string ), i => i );
         _typeToConverterFunction.Add( typeof( int ), i => Int32.Parse( i ) );
         _typeToConverterFunction.Add( typeof( int? ), i => Int32.Parse( i ) );
         _typeToConverterFunction.Add( typeof( double ), i => Double.Parse( i ) );
         _typeToConverterFunction.Add( typeof( double? ), i => Double.Parse( i ) );
         _typeToConverterFunction.Add( typeof( byte[] ), Convert.FromBase64String );
         _typeToConverterFunction.Add( typeof( Guid ), i => Guid.Parse( i ) );
         _typeToConverterFunction.Add( typeof( Guid? ), i => Guid.Parse( i ) );
         _typeToConverterFunction.Add( typeof( DateTime ), i => DateTime.Parse( i, CultureInfo.CurrentCulture, DateTimeStyles.AdjustToUniversal ) );
         _typeToConverterFunction.Add( typeof( DateTime? ), i => DateTime.Parse( i, CultureInfo.CurrentCulture, DateTimeStyles.AdjustToUniversal ) );
         _typeToConverterFunction.Add( typeof( bool ), i => Boolean.Parse( i ) );
         _typeToConverterFunction.Add( typeof( bool? ), i => Boolean.Parse( i ) );
         _typeToConverterFunction.Add( typeof( long ), i => Int64.Parse( i ) );
         _typeToConverterFunction.Add( typeof( long? ), i => Int64.Parse( i ) );
         _typeToConverterFunction.Add( typeof( Uri ), i => new Uri( i ) );
      }

      public T CreateInstanceFromProperties<T>() where T : new()
      {
         var newItem = new T();

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

         foreach ( var property in newItem.GetType().GetProperties().Where( p => p.ShouldSerialize() ) )
         {
            var valueToConvert = this[property.Name];
            var convertedValue = TypeConverter( valueToConvert, property.PropertyType );
            property.SetValue( newItem, convertedValue, null );
         }

         return newItem;
      }

      private static object TypeConverter( EntityPropertyInfo itemToConvert, Type type )
      {
         if ( itemToConvert.IsNull )
         {
            return null;
         }

         var funcToCall = _typeToConverterFunction[type];

         return funcToCall( itemToConvert.Value.ToString() );
      }

      public static GenericEntity HydrateFrom<T>( T sourceItem ) where T : new()
      {
         string partitionKey = sourceItem.ReadPropertyDecoratedWith<PartitionKeyAttribute,string>();
         string rowKey = sourceItem.ReadPropertyDecoratedWith<RowKeyAttribute,string>();

         return HydrateFrom( sourceItem, partitionKey, rowKey );
      }

      public static GenericEntity HydrateFrom<T>( T sourceItem, string partitionKey, string rowKey ) where T : new()
      {
         var genericEntity = new GenericEntity();
         foreach ( var property in sourceItem.GetType().GetProperties().Where( p => p.ShouldSerialize() ) )
         {
            if ( InvalidPropertyNames.Contains( property.Name ) )
            {
               throw new InvalidEntityException( string.Format( "Invalid property name {0}", property.Name ) );
            }
            var valueOfProperty = property.GetValue( sourceItem, null );
            genericEntity[property.Name] = new EntityPropertyInfo( valueOfProperty, property.PropertyType, valueOfProperty == null );
         }

         genericEntity.PartitionKey = partitionKey;
         genericEntity.RowKey = rowKey;

         return genericEntity;
      }

      public bool AreTheseEqual( GenericEntity rightSide )
      {
         return GetProperties()
            .All( propertyInfo => propertyInfo.Value == rightSide.GetProperties()[propertyInfo.Key].Value );
      }
   }
}