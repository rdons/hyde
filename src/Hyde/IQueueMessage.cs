namespace TechSmith.Hyde
{
   public interface IQueueMessage
   {
      string Message { get; }
      bool IsPoison( IPoisonLimitProvider poisonLimitProvider );
   }
}