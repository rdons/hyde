namespace TechSmith.Hyde
{
   public class GenericPoisonLimitProvider : IPoisonLimitProvider
   {
      public int Limit
      {
         get
         {
            return 3;
         }
      }
   }
}