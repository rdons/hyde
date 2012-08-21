using System.Collections.Generic;

namespace TechSmith.Hyde
{
   public interface IQueueNameProvider
   {
      IEnumerable<string> Names { get; }
   }
}