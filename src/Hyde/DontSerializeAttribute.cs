using System;

namespace TechSmith.Hyde
{

   [AttributeUsage( AttributeTargets.Property )]
   public class DontSerializeAttribute : Attribute
   {
   }
}
