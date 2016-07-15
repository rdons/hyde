namespace TechSmith.Hyde.Table
{
   public enum ConflictHandling
   {
       /// <summary>
      /// Respects ETag, if one exists
      /// </summary>
      Throw,

      /// <summary>
      /// Ignores ETag mismatch, and completes operation
      /// </summary>
      Overwrite,
   }
}
