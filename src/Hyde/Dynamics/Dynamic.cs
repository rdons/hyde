using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.CSharp.RuntimeBinder;
using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

namespace TechSmith.Hyde.Dynamics
{
   // borrowed from https://github.com/ekonbenefits/dynamitey
   // License: DynamicsLicense.txt
   // Modifications made to work with dotnet core
   public class Dynamic
   {
      public static IEnumerable<string> GetMemberNames( object target, bool dynamicOnly = false )
      {
         var tList = new List<string>();
         if ( !dynamicOnly )
         {
            tList.AddRange( target.GetType().GetTypeInfo().GetProperties().Select( it => it.Name ) );
         }

         var tTarget = target as IDynamicMetaObjectProvider;
         if ( tTarget != null )
         {
            tList.AddRange( tTarget.GetMetaObject( Expression.Constant( tTarget ) ).GetDynamicMemberNames() );
         }
         return tList;
      }

      public static dynamic InvokeGet( object target, string name )
      {
         Type tContext;
         bool tStaticContext;
         target = target.GetTargetContext( out tContext, out tStaticContext );
         CallSite tSite = null;
         return InvokeHelper.InvokeGetCallSite( target, name, tContext, tStaticContext, ref tSite );
      }
   }

   public static class InvokeHelper
   {
      internal const int Unknown = 0;
      internal const int KnownGet = 1;
      internal const int KnownSet = 2;
      internal const int KnownMember = 3;
      internal const int KnownDirect = 4;
      internal const int KnownConstructor = 5;

      internal delegate CallSiteBinder LazyBinder();

      internal static object InvokeGetCallSite( object target, string name, Type context, bool staticContext, ref CallSite callsite )
      {

         if ( callsite == null )
         {
            var tTargetFlag = CSharpArgumentInfoFlags.None;
            LazyBinder tBinder;
            Type tBinderType;
            int tKnownType;
            if ( staticContext ) //CSharp Binder won't call Static properties, grrr.
            {
               var tStaticFlag = CSharpBinderFlags.None;
               if ( ( target is Type && ( (Type) target ).GetTypeInfo().IsPublic ) || Util.IsMono )
               {
                  //Mono only works if InvokeSpecialName is set and .net only works if it isn't
                  if ( Util.IsMono )
                     tStaticFlag |= CSharpBinderFlags.InvokeSpecialName;

                  tBinder = () => Binder.InvokeMember( tStaticFlag, "get_" + name,
                                                      null,
                                                      context,
                                                      new List<CSharpArgumentInfo>
                                                          {
                                                                    CSharpArgumentInfo.Create(
                                                                        CSharpArgumentInfoFlags.IsStaticType |
                                                                        CSharpArgumentInfoFlags.UseCompileTimeType,
                                                                        null)
                                                          } );

                  tBinderType = typeof( InvokeMemberBinder );
                  tKnownType = KnownMember;
               }
               else
               {

                  tBinder = () => Binder.GetMember( tStaticFlag, name,
                                                      context,
                                                      new List<CSharpArgumentInfo>
                                                          {
                                                                    CSharpArgumentInfo.Create(
                                                                        CSharpArgumentInfoFlags.IsStaticType, null)
                                                          } );

                  tBinderType = typeof( InvokeMemberBinder );
                  tKnownType = KnownMember;
               }
            }
            else
            {

               tBinder = () => Binder.GetMember( CSharpBinderFlags.None, name,
                                                  context,
                                                  new List<CSharpArgumentInfo>
                                                      {
                                                              CSharpArgumentInfo.Create(
                                                                  tTargetFlag, null)
                                                      } );
               tBinderType = typeof( GetMemberBinder );
               tKnownType = KnownGet;
            }


            callsite = CreateCallSite<Func<CallSite, object, object>>( tBinderType, tKnownType, tBinder, name, context,
                            staticContext: staticContext );
         }
         var tCallSite = (CallSite<Func<CallSite, object, object>>) callsite;
         return tCallSite.Target( tCallSite, target );

      }

      private static readonly object _binderCacheLock = new object();

      internal static CallSite<T> CreateCallSite<T>(
       Type specificBinderType,
       int knownType,
       LazyBinder binder,
       InvokeMemberName name,
       Type context,
       string[] argNames = null,
       bool staticContext = false,
       bool isEvent = false
       )
       where T : class
      {
         var tHash = BinderHash<T>.Create( name, context, argNames, specificBinderType, staticContext, isEvent, knownType != Unknown );
         lock ( _binderCacheLock )
         {
            CallSite<T> tOut;
            if ( !TryDynamicCachedCallSite( tHash, knownType, out tOut ) )
            {
               tOut = CallSite<T>.Create( binder() );
               SetDynamicCachedCallSite( tHash, knownType, tOut );
            }
            return tOut;
         }
      }

      internal static class BinderCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>> Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      internal static class BinderGetCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>>
                 Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      internal static class BinderConstructorCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>> Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      internal static class BinderSetCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>> Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      internal static class BinderMemberCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>> Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      internal static class BinderDirectCache<T> where T : class
      {
         internal static readonly IDictionary<BinderHash<T>, CallSite<T>> Cache = new Dictionary<BinderHash<T>, CallSite<T>>();
      }

      private static bool TryDynamicCachedCallSite<T>( BinderHash<T> hash, int knownBinderType, out CallSite<T> callSite ) where T : class
      {
         switch ( knownBinderType )
         {
            default:
            return BinderCache<T>.Cache.TryGetValue( hash, out callSite );

            case KnownGet:
            return BinderGetCache<T>.Cache.TryGetValue( hash, out callSite );

            case KnownSet:
            return BinderSetCache<T>.Cache.TryGetValue( hash, out callSite );

            case KnownMember:
            return BinderMemberCache<T>.Cache.TryGetValue( hash, out callSite );

            case KnownDirect:
            return BinderDirectCache<T>.Cache.TryGetValue( hash, out callSite );

            case KnownConstructor:
            return BinderConstructorCache<T>.Cache.TryGetValue( hash, out callSite );
         }
      }

      private static void SetDynamicCachedCallSite<T>( BinderHash<T> hash, int knownBinderType, CallSite<T> callSite ) where T : class
      {
         switch ( knownBinderType )
         {
            default:
            BinderCache<T>.Cache[hash] = callSite;
            break;
            case KnownGet:
            BinderGetCache<T>.Cache[hash] = callSite;
            break;
            case KnownSet:
            BinderSetCache<T>.Cache[hash] = callSite;
            break;
            case KnownMember:
            BinderMemberCache<T>.Cache[hash] = callSite;
            break;
            case KnownDirect:
            BinderDirectCache<T>.Cache[hash] = callSite;
            break;
            case KnownConstructor:
            BinderConstructorCache<T>.Cache[hash] = callSite;
            break;
         }
      }
   }

   internal class BinderHash
   {


      protected BinderHash( Type delegateType, String name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
      {
         KnownBinder = knownBinder;
         BinderType = binderType;
         StaticContext = staticContext;
         DelegateType = delegateType;
         Name = name;
         IsSpecialName = false;
         GenericArgs = null;
         Context = context;
         ArgNames = argNames;
         IsEvent = isEvent;


      }

      protected BinderHash( Type delegateType, InvokeMemberName name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
      {
         KnownBinder = knownBinder;
         BinderType = binderType;
         StaticContext = staticContext;
         DelegateType = delegateType;
         Name = name.Name;
         IsSpecialName = name.IsSpecialName;
         GenericArgs = name.GenericArgs;
         Context = context;
         ArgNames = argNames;
         IsEvent = isEvent;


      }




      public bool KnownBinder
      {
         get; protected set;
      }
      public Type BinderType
      {
         get; protected set;
      }
      public bool StaticContext
      {
         get; protected set;
      }
      public bool IsEvent
      {
         get; protected set;
      }
      public Type DelegateType
      {
         get; protected set;
      }
      public string Name
      {
         get; protected set;
      }
      public bool IsSpecialName
      {
         get; protected set;
      }
      public Type[] GenericArgs
      {
         get; protected set;
      }
      public Type Context
      {
         get; protected set;
      }
      public string[] ArgNames
      {
         get; protected set;
      }

      public virtual bool Equals( BinderHash other )
      {
         if ( ReferenceEquals( null, other ) )
            return false;
         if ( ReferenceEquals( this, other ) )
            return true;

         var tArgNames = ArgNames;
         var tOtherArgNames = other.ArgNames;
         var tGenArgs = GenericArgs;
         var tOtherGenArgs = other.GenericArgs;

         return
             !( tOtherArgNames == null ^ tArgNames == null )
             && other.IsEvent == IsEvent
             && other.StaticContext == StaticContext
             && Equals( other.Context, Context )
             && ( KnownBinder || Equals( other.BinderType, BinderType ) )
             && Equals( other.DelegateType, DelegateType )
             && Equals( other.Name, Name )
             && !( other.IsSpecialName ^ IsSpecialName )
             && !( tOtherGenArgs == null ^ tGenArgs == null )
             && ( tGenArgs == null ||
             //Exclusive Or makes sure this doesn't happen
             // ReSharper disable AssignNullToNotNullAttribute
             tGenArgs.SequenceEqual( tOtherGenArgs ) )
             // ReSharper restore AssignNullToNotNullAttribute
             && ( tArgNames == null
                              // ReSharper disable AssignNullToNotNullAttribute
                              //Exclusive Or Makes Sure this doesn't happen

                              || tOtherArgNames.SequenceEqual( tArgNames ) );
         // ReSharper restore AssignNullToNotNullAttribute
      }


      public override bool Equals( object obj )
      {
         if ( ReferenceEquals( null, obj ) )
            return false;
         if ( ReferenceEquals( this, obj ) )
            return true;
         if ( !( obj is BinderHash ) )
            return false;
         return Equals( (BinderHash) obj );
      }

      public override int GetHashCode()
      {
         unchecked
         {
            var tArgNames = ArgNames;

            int result = ( tArgNames == null ? 0 : tArgNames.Length * 397 );
            result = ( result ^ StaticContext.GetHashCode() );
            //result = (result * 397) ^ DelegateType.GetHashCode();
            //result = (result * 397) ^ Context.GetHashCode();
            result = ( result * 397 ) ^ Name.GetHashCode();
            return result;
         }
      }
   }


   public abstract class String_OR_InvokeMemberName
   {
      /// <summary>
      /// Performs an implicit conversion from <see cref="System.String"/> to <see cref="String_OR_InvokeMemberName"/>.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <returns>The result of the conversion.</returns>
      public static implicit operator String_OR_InvokeMemberName( string name )
      {
         return new InvokeMemberName( name, null );
      }


      /// <summary>
      /// Gets the name.
      /// </summary>
      /// <value>The name.</value>
      public string Name
      {
         get; protected set;
      }
      /// <summary>
      /// Gets the generic args.
      /// </summary>
      /// <value>The generic args.</value>
      public Type[] GenericArgs
      {
         get; protected set;
      }

      /// <summary>
      /// Gets or sets a value indicating whether this member is special name.
      /// </summary>
      /// <value>
      /// 	<c>true</c> if this instance is special name; otherwise, <c>false</c>.
      /// </value>
      public bool IsSpecialName
      {
         get; protected set;
      }
   }

   /// <summary>
   /// Name of Member with associated Generic parameterss
   /// </summary>
   public sealed class InvokeMemberName : String_OR_InvokeMemberName
   {
      /// <summary>
      /// Performs an implicit conversion from <see cref="System.String"/> to <see cref="InvokeMemberName"/>.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <returns>The result of the conversion.</returns>
      public static implicit operator InvokeMemberName( string name )
      {
         return new InvokeMemberName( name, null );
      }


      /// <summary>
      /// Initializes a new instance of the <see cref="InvokeMemberName"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="genericArgs">The generic args.</param>
      public InvokeMemberName( string name, params Type[] genericArgs )
      {
         Name = name;
         GenericArgs = genericArgs;
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="InvokeMemberName"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="isSpecialName">if set to <c>true</c> [is special name].</param>
      public InvokeMemberName( string name, bool isSpecialName )
      {
         Name = name;
         GenericArgs = new Type[] { };
         IsSpecialName = isSpecialName;
      }

      /// <summary>
      /// Equalses the specified other.
      /// </summary>
      /// <param name="other">The other.</param>
      /// <returns></returns>
      public bool Equals( InvokeMemberName other )
      {
         if ( ReferenceEquals( null, other ) )
            return false;
         if ( ReferenceEquals( this, other ) )
            return true;
         return EqualsHelper( other );
      }

      private bool EqualsHelper( InvokeMemberName other )
      {

         var tGenArgs = GenericArgs;
         var tOtherGenArgs = other.GenericArgs;


         return Equals( other.Name, Name )
             && !( other.IsSpecialName ^ IsSpecialName )
             && !( tOtherGenArgs == null ^ tGenArgs == null )
             && ( tGenArgs == null ||
             //Exclusive Or makes sure this doesn't happen
             // ReSharper disable AssignNullToNotNullAttribute
             tGenArgs.SequenceEqual( tOtherGenArgs ) );
         // ReSharper restore AssignNullToNotNullAttribute
      }

      /// <summary>
      /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
      /// </summary>
      /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
      /// <returns>
      /// 	<c>true</c> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <c>false</c>.
      /// </returns>
      public override bool Equals( object obj )
      {
         if ( ReferenceEquals( null, obj ) )
            return false;
         if ( ReferenceEquals( this, obj ) )
            return true;
         if ( !( obj is InvokeMemberName ) )
            return false;
         return EqualsHelper( (InvokeMemberName) obj );
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
      /// </returns>
      public override int GetHashCode()
      {
         unchecked
         {
            return ( GenericArgs != null ? GenericArgs.Length.GetHashCode() * 397 : 0 ) ^ ( Name.GetHashCode() );
         }
      }
   }


   internal class BinderHash<T> : BinderHash where T : class
   {
      public static BinderHash<T> Create( string name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
      {
         return new BinderHash<T>( name, context, argNames, binderType, staticContext, isEvent, knownBinder );
      }

      public static BinderHash<T> Create( InvokeMemberName name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
      {
         return new BinderHash<T>( name, context, argNames, binderType, staticContext, isEvent, knownBinder );
      }

      protected BinderHash( InvokeMemberName name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
          : base( typeof( T ), name, context, argNames, binderType, staticContext, isEvent, knownBinder )
      {
      }

      protected BinderHash( string name, Type context, string[] argNames, Type binderType, bool staticContext, bool isEvent, bool knownBinder )
          : base( typeof( T ), name, context, argNames, binderType, staticContext, isEvent, knownBinder )
      {
      }

      public override bool Equals( BinderHash other )
      {

         if ( other is BinderHash<T> )
         {
            var tGenArgs = GenericArgs;
            var tOtherGenArgs = other.GenericArgs;

            return
                   !( other.ArgNames == null ^ ArgNames == null )
                   && other.IsEvent == IsEvent
                   && other.StaticContext == StaticContext
                   && ( KnownBinder || Equals( other.BinderType, BinderType ) )
                   && Equals( other.Context, Context )
                   && Equals( other.Name, Name )
                    && !( other.IsSpecialName ^ IsSpecialName )
                    && !( tOtherGenArgs == null ^ tGenArgs == null )
                    && ( tGenArgs == null ||
                    //Exclusive Or makes sure this doesn't happen
                    // ReSharper disable AssignNullToNotNullAttribute
                    tGenArgs.SequenceEqual( tOtherGenArgs ) )
                   // ReSharper restore AssignNullToNotNullAttribute
                   && ( ArgNames == null
                         // ReSharper disable AssignNullToNotNullAttribute
                         //Exclusive Or Makes Sure this doesn't happen
                         || other.ArgNames.SequenceEqual( ArgNames ) );
            // ReSharper restore AssignNullToNotNullAttribute
         }
         return false;


      }
   }

   public static class Util
   {
      /// <summary>
      /// Is Current Runtime Mono?
      /// </summary>
      public static readonly bool IsMono;

      static Util()
      {
         IsMono = Type.GetType( "Mono.Runtime" ) != null;


      }

      /// <summary>
      /// Gets the target context.
      /// </summary>
      /// <param name="target">The target.</param>
      /// <param name="context">The context.</param>
      /// <param name="staticContext">if set to <c>true</c> [static context].</param>
      /// <returns></returns>
      public static object GetTargetContext( this object target, out Type context, out bool staticContext )
      {
         var tInvokeContext = target as InvokeContext;
         staticContext = false;
         if ( tInvokeContext != null )
         {
            staticContext = tInvokeContext.StaticContext;
            context = tInvokeContext.Context;
            context = context.FixContext();
            return tInvokeContext.Target;
         }

         context = target as Type ?? target.GetType();
         context = context.FixContext();
         return target;
      }


      /// <summary>
      /// Fixes the context.
      /// </summary>
      /// <param name="context">The context.</param>
      /// <returns></returns>
      public static Type FixContext( this Type context )
      {
         if ( context.IsArray )
         {
            return typeof( object );
         }
         return context;
      }
   }

   /// <summary>
   /// Specific version of InvokeContext which declares a type to be used to invoke static methods.
   /// </summary>
   public class StaticContext : InvokeContext
   {
      /// <summary>
      /// Performs an explicit conversion from <see cref="System.Type"/> to <see cref="Dynamitey.StaticContext"/>.
      /// </summary>
      /// <param name="type">The type.</param>
      /// <returns>The result of the conversion.</returns>
      public static explicit operator StaticContext( Type type )
      {
         return new StaticContext( type );
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="StaticContext"/> class.
      /// </summary>
      /// <param name="target">The target.</param>
      public StaticContext( Type target ) : base( target, true, null )
      {
      }
   }

   /// <summary>
   /// Object that stores a context with a target for dynamic invocation
   /// </summary>
   public class InvokeContext
   {
      /// <summary>
      /// Gets or sets the target.
      /// </summary>
      /// <value>The target.</value>
      public object Target
      {
         get; protected set;
      }
      /// <summary>
      /// Gets or sets the context.
      /// </summary>
      /// <value>The context.</value>
      public Type Context
      {
         get; protected set;
      }

      /// <summary>
      /// Gets or sets a value indicating whether [static context].
      /// </summary>
      /// <value><c>true</c> if [static context]; otherwise, <c>false</c>.</value>
      public bool StaticContext
      {
         get; protected set;
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="InvokeContext"/> class.
      /// </summary>
      /// <param name="target">The target.</param>
      /// <param name="staticContext">if set to <c>true</c> [static context].</param>
      /// <param name="context">The context.</param>
      public InvokeContext( Type target, bool staticContext, object context )
      {
         if ( context != null && !( context is Type ) )
         {
            context = context.GetType();
         }
         Target = target;
         Context = ( (Type) context ) ?? target;
         StaticContext = staticContext;
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="InvokeContext"/> class.
      /// </summary>
      /// <param name="target">The target.</param>
      /// <param name="context">The context.</param>
      public InvokeContext( object target, object context )
      {
         this.Target = target;

         if ( context != null && !( context is Type ) )
         {
            context = context.GetType();
         }

         Context = (Type) context;
      }
   }
}
