using System;

namespace TechSmith.Hyde
{
   /// <summary>
   /// Marks a property as the partition key. The property must have string type.
   /// </summary>
   [AttributeUsage( AttributeTargets.Property, AllowMultiple = false, Inherited = true )]
   public class PartitionKeyAttribute : DontSerializeAttribute
   {
   }
}
