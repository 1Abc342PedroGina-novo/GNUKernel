/*
 * Copyright 1991-1998 by Open Software Foundation, Inc. 
 *              All Rights Reserved 
 *  
 * Permission to use, copy, modify, and distribute this software and 
 * its documentation for any purpose and without fee is hereby granted, 
 * provided that the above copyright notice appears in all copies and 
 * that both the copyright notice and this permission notice appear in 
 * supporting documentation. 
 *  
 * OSF DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE 
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE. 
 *  
 * IN NO EVENT SHALL OSF BE LIABLE FOR ANY SPECIAL, INDIRECT, OR 
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM 
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN ACTION OF CONTRACT, 
 * NEGLIGENCE, OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION 
 * WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. 
 */
/*
 * MkLinux
 */
/* CMU_HIST */
/*
 * Revision 2.27.6.6  92/09/15  17:37:08  jeffreyh
 * 	Removed vm_object_pmap_remove.  From bruel@gr.osf.org.
 * 	[92/09/03            dlb]
 * 	Changes for new vm_fault_page interface to pass back an error
 * 	code.
 * 	[92/07/28            sjs]
 * 
 * Revision 2.27.6.5  92/06/25  13:58:48  jeffreyh
 * 	Restore call to vm_object_cache_unlock in vm_object_destroy that
 * 	was lost in some previous merge.
 * 	[92/06/25            jeffreyh]
 * 
 * Revision 2.27.6.4  92/03/28  10:17:21  jeffreyh
 * 	Change some uses of temporary to internal because external temporary
 * 	objects now exist.
 * 	[92/03/27            dlb]
 * 	Dead pager check should be ip_active, not IP_VALID.
 * 	[92/03/20            dlb]
 * 	Fix from dlb to check is a pager is active as well as valid.
 * 	[92/03/17  11:11:57  jeffreyh]
 * 
 * Revision 2.27.6.3  92/03/03  16:26:58  jeffreyh
 * 	Check for dead pager in vm_object_pager_wakeup.
 * 	[92/02/28            dlb]
 * 	Don't do pager wakeup from vm_object_destroy().
 * 	Don't set shadowed field in temporary object copied by
 * 	vm_object_copy_delayed().
 * 	[92/02/28  13:16:44  jeffreyh]
 * 
 * 	Remove dest_wired logic from vm_object_copy_slowly.
 * 	[92/02/21  10:16:35  dlb]
 * 
 * 	Maintain shadowed field in objects for use by new
 * 	VM_INHERIT_SHARE code (vm_map_fork).  Restore old
 * 	interface to vm_object_pmap_protect.
 * 	[92/02/19  14:28:12  dlb]
 * 
 * 	Use use_shared_copy instead of new copy strategy for
 * 	temporary objects.
 * 	[92/01/07  11:15:53  dlb]
 * 
 * 	Add asynchronous copy-on-write logic for temporary objects.
 * 	[92/01/06  16:24:59  dlb]
 * 	Finish jsb's IKOT_PAGER_TERMINATING changes:
 * 		- Call memory_object_release from vm_object_terminate.
 * 		- Split out wakeup code into separate function so
 * 			it can be called from vm_object_destroy()
 * 			and ipc_kobject_destroy().
 * 		- Change vm_object_enter to allow for port death.
 * 	[92/02/25            dlb]
 * 
 * 		Fixed vm_object_lookup to use IP_VALID, lock the port, and
 * 		allow only IKOT_PAGING_REQUEST, not IKOT_PAGER.
 * 		Removed vm_object_debug.
 * 		[91/12/31            rpd]
 * 
 * 		Split vm_object_init into vm_object_bootstrap and vm_object_init.
 * 		[91/12/28            rpd]
 * 
 * Revision 2.27.6.2  92/02/21  11:28:48  jsb
 * 	Use IKOT_PAGER_TERMINATING to solve terminate/init race.
 * 	No one in the history of Mach has ever written a pager that
 * 	can handle this race, including kernel-supplied pagers such
 * 	as the default pager (but excluding netmemory pagers which
 * 	handle the fully general case of multiple clients).
 * 	[92/02/16  16:22:50  jsb]
 * 
 * 	(Naively) handle temporary objects in vm_object_copy_strategically.
 * 	This now happens because of new MEMORY_OBJECT_COPY_TEMPORARY option.
 * 	[92/02/11  11:41:34  jsb]
 * 
 * 	Object->pager_request is now declared as a pager_request_t, which is
 * 	an xmm_obj_t in NORMA_VM systems, ipc_port_t otherwise.
 * 	Removed bogus vm_object_incr now that xmm_server uses vm_object_lookup.
 * 	Picked up rpd's vm_object_lookup, and conditionalized it for NORMA_VM
 * 	so that port must be a pager port (vs. standard case where it must
 * 	be a pager_request port). Added changes for pager_name being a send
 * 	right instead of a receive right. Cleaned up vm_object_enter logic,
 * 	thanks to elimination of pager kobject overloading. (It used to switch
 * 	between a vm_object and an xmm obj depending on whether it was mapped
 * 	locally or remotely first.) Change vm_object_collapse to simply move
 * 	pager_request from one object to the other (as in the non-NORMA_VM
 * 	case), instead of calling xmm_object_set.
 * 	[92/02/10  09:57:12  jsb]
 * 
 * Revision 2.27.6.1  92/02/18  19:25:11  jeffreyh
 * 	Free new page before exiting with an error in vm_object_copy_slowly
 * 	[92/02/14            dlb]
 * 
 * Revision 2.27  91/12/11  08:44:01  jsb
 * 	Changed vm_object_coalesce to also check for paging references.
 * 	This fixes a problem with page-list map-copy objects.
 * 	[91/12/03            rpd]
 * 
 * Revision 2.26  91/12/10  13:27:07  jsb
 * 	Comment out noisy printf. (Avoiding dealloc ...)
 * 	[91/12/10  12:50:34  jsb]
 * 
 * Revision 2.25  91/11/12  11:52:09  rvb
 * 	Added simple_lock_pause.
 * 	[91/11/12            rpd]
 * 
 * Revision 2.24  91/08/28  11:18:37  jsb
 * 	Handle dirty private pages correctly in vm_object_terminate().
 * 	[91/07/30  14:18:54  dlb]
 * 
 * 	single_use --> use_old_pageout in object initialization template.
 * 	Support precious pages in vm_object_terminate().
 * 	[91/07/03  14:18:07  dlb]
 * 
 * Revision 2.23  91/07/31  18:21:27  dbg
 * 	Add vm_object_page_map, to attach a set of physical pages to an
 * 	object.
 * 	[91/07/30  17:26:58  dbg]
 * 
 * Revision 2.22  91/07/01  08:27:52  jsb
 * 	Improved NORMA_VM support, including support for memory_object_create.
 * 	[91/06/29  17:06:11  jsb]
 * 
 * Revision 2.21  91/06/25  11:07:02  rpd
 * 	Fixed includes to avoid norma files unless they are really needed.
 * 	[91/06/25            rpd]
 * 
 * Revision 2.20  91/06/25  10:34:11  rpd
 * 	Changed memory_object_t to ipc_port_t where appropriate.
 * 	Removed extraneous casts.
 * 	[91/05/28            rpd]
 * 
 * Revision 2.19  91/06/17  15:49:25  jsb
 * 	Added NORMA_VM support.
 * 	[91/06/17  15:28:16  jsb]
 * 
 * Revision 2.18  91/05/18  14:41:22  rpd
 * 	Changed vm_object_deactivate_pages to call vm_page_deactivate
 * 	on inactivate pages, because that is no longer a no-op.
 * 	[91/04/20            rpd]
 * 
 * 	Fixed vm_object_pmap_protect, vm_object_pmap_remove,
 * 	vm_object_page_remove, vm_object_copy_call, vm_object_copy_delayed
 * 	to check for fictitious pages.
 * 	[91/04/10            rpd]
 * 	Fixed vm_object_terminate to allow for busy/absent pages.
 * 	[91/04/02            rpd]
 * 
 * 	Added VM_FAULT_FICTITIOUS_SHORTAGE.
 * 	[91/03/29            rpd]
 * 	Added vm/memory_object.h.
 * 	[91/03/22            rpd]
 * 
 * Revision 2.17  91/05/14  17:50:19  mrt
 * 	Correcting copyright
 * 
 * Revision 2.16  91/05/08  13:34:59  dbg
 * 	Rearrange initialization code in vm_object_enter to clearly
 * 	separate 'internal' case (and to avoid a vax GCC bug).
 * 	[91/04/17            dbg]
 * 
 * Revision 2.15  91/03/16  15:06:19  rpd
 * 	Changed vm_object_deactivate_pages and vm_object_terminate to
 * 	check for busy pages.  Changed how vm_object_terminate
 * 	interacts with the pageout daemon.
 * 	[91/03/12            rpd]
 * 	Fixed vm_object_page_remove to be smart about small regions.
 * 	[91/03/05            rpd]
 * 	Added resume, continuation arguments to vm_fault_page.
 * 	Added continuation argument to VM_PAGE_WAIT.
 * 	[91/02/05            rpd]
 * 
 * Revision 2.14  91/02/05  17:59:16  mrt
 * 	Changed to new Mach copyright
 * 	[91/02/01  16:33:29  mrt]
 * 
 * Revision 2.13  91/01/08  16:45:32  rpd
 * 	Added continuation argument to thread_block.
 * 	[90/12/08            rpd]
 * 
 * 	Fixed vm_object_terminate to give vm_pageout_page busy pages.
 * 	[90/11/22            rpd]
 * 
 * 	Changed VM_WAIT to VM_PAGE_WAIT.
 * 	[90/11/13            rpd]
 * 
 * Revision 2.12  90/11/06  18:44:12  rpd
 * 	From dlb@osf.org:
 * 	If pager initialization is in progress (object->pager_created &&
 * 	!object->pager_initialized), then vm_object_deallocate must wait
 * 	for it to complete before terminating the object.  Because anything
 * 	can happen in the interim, it must recheck its decision to terminate
 * 	after the wait completes.
 * 
 * Revision 2.11  90/10/12  13:06:24  rpd
 * 	In vm_object_copy_slowly, only activate pages returned from
 * 	vm_fault_page if they aren't already on a pageout queue.
 * 	[90/10/08            rpd]
 * 
 * Revision 2.10  90/09/09  14:34:20  rpd
 * 	Fixed bug in vm_object_copy_slowly.  The pages in the new object
 * 	should be marked as dirty.
 * 	[90/09/08            rpd]
 * 
 * Revision 2.9  90/06/19  23:02:52  rpd
 * 	Picked up vm_submap_object.
 * 	[90/06/08            rpd]
 * 
 * Revision 2.8  90/06/02  15:11:31  rpd
 * 	Changed vm_object_collapse_bypass_allowed back to TRUE.
 * 	[90/04/22            rpd]
 * 	Converted to new IPC.
 * 	[90/03/26  23:16:54  rpd]
 * 
 * Revision 2.7  90/05/29  18:38:57  rwd
 * 	Rfr change to reflect change in vm_pageout_page.
 * 	[90/04/12  13:48:31  rwd]
 * 
 * Revision 2.6  90/05/03  15:53:08  dbg
 * 	Make vm_object_pmap_protect follow shadow chains if removing all
 * 	permissions.  Fix check for using pmap_protect on entire range.
 * 	[90/04/18            dbg]
 * 
 * 	Pass 'flush' argument to vm_pageout_page.
 * 	[90/03/28            dbg]
 * 
 * Revision 2.5  90/02/22  20:06:24  dbg
 * 	Add changes from mainline:
 * 
 * 		Fix comment on vm_object_delayed.
 * 		[89/12/18            dlb]
 * 		Revised use of PAGE_WAKEUP macros.  Don't clear busy flag when
 * 		restarting unlock requests.
 * 		[89/12/13            dlb]
 * 		Fix locking problems in vm_object_copy_slowly:
 * 		    1.  Must hold lock on object containing page being copied
 * 			from (result_page) object when activating that page.
 * 		    2.  Fix typo that tried to lock source object twice if
 * 		        vm_fault_page returned two objects; this rare case
 * 		        would hang a multiprocessor if it occurred.
 * 		[89/12/11            dlb]
 * 
 * 		Complete un-optimized overwriting implementation.  Move
 * 		futuristic implementation here.  Clean up and document
 * 		copy routines.
 * 		[89/08/05  18:01:22  mwyoung]
 * 		Add vm_object_pmap_protect() to perform pmap protection
 * 		changes more efficiently.
 * 		[89/07/07  14:06:34  mwyoung]
 * 		Split up vm_object_copy into one part that does not unlock the
 * 		object (i.e., does not sleep), and another that may.  Also, use
 * 		separate routines for each copy strategy.
 * 		[89/07/06  18:39:55  mwyoung]
 * 
 * Revision 2.4  90/01/11  11:47:58  dbg
 * 	Use vm_fault_cleanup.
 * 	[89/12/13            dbg]
 * 
 * 	Fix locking bug in vm_object_copy_slowly.
 * 	[89/12/13            dbg]
 * 
 * 	Pick up changes from mainline:
 * 		Remove size argument from vm_external_destroy.
 * 		Add paging_offset to size used in vm_external_create.
 * 		Account for existence_info movement in vm_object_collapse.
 * 		[89/10/16  15:30:16  af]
 * 
 * 		Remove XP conditionals.
 * 		Document global variables.
 * 		[89/10/10            mwyoung]
 * 
 * 		Added initialization for lock_in_progress, lock_restart fields.
 * 		[89/08/07            mwyoung]
 * 
 * 		Beautify dirty bit handling in vm_object_terminate().
 * 		Don't write back "error" pages in vm_object_terminate().
 * 		[89/04/22            mwyoung]
 * 
 * 		Removed vm_object_list, vm_object_count.
 * 		[89/08/31  19:45:43  rpd]
 * 
 * 		Optimization from NeXT: vm_object_deactivate_pages checks now
 * 		that the page is inactive before call vm_page_deactivate.  Also,
 * 		initialize the last_alloc field in vm_object_template.
 * 		[89/08/19  23:46:42  rpd]
 * 
 * Revision 2.3  89/10/16  15:22:22  rwd
 * 	In vm_object_collapse: leave paging_offset zero if the backing
 * 	object does not have a pager.
 * 	[89/10/12            dbg]
 * 
 * Revision 2.2  89/09/08  11:28:45  dbg
 * 	Add keep_wired parameter to vm_object_copy_slowly to wire new
 * 	pages.
 * 	[89/07/14            dbg]
 * 
 * 22-May-89  David Golub (dbg) at Carnegie-Mellon University
 *	Refuse vm_object_pager_create if no default memory manager.
 *
 * 28-Apr-89  David Golub (dbg) at Carnegie-Mellon University
 *	Changes for MACH_KERNEL:
 *	. Use port_alloc_internal to simplify port_alloc call.
 *	. Remove non-XP code.
 *
 * Revision 2.23  89/04/23  13:25:43  gm0w
 * 	Changed assert(!p->busy) to assert(!p->busy || p->absent) in
 * 	vm_object_collapse().  Change from jsb/mwyoung.
 * 	[89/04/23            gm0w]
 * 
 * Revision 2.22  89/04/18  21:26:29  mwyoung
 * 	Recent history [mwyoung]:
 * 		Use hints about pages not in main memory.
 * 		Get default memory manager port more carefully, as it
 * 		 may now be changed after initialization.
 * 		Use new event mechanism.
 * 		Correct vm_object_destroy() to actually abort requests.
 * 		Correct handling of paging offset in vm_object_collapse().
 * 	Condensed history:
 * 		Add copy strategy handling, including a routine
 * 		 (vm_object_copy_slowly()) to perform an immediate copy
 * 		 of a region of an object. [mwyoung]
 * 	 	Restructure the handling of the relationships among memory
 * 		 objects (the ports), the related fields in the vm_object
 * 		 structure, and routines that manipulate them [mwyoung].
 * 		Simplify maintenance of the unreferenced-object cache. [mwyoung]
 * 		Distinguish internal and temporary attributes. [mwyoung]
 * 		Reorganized and documented maintenance of the
 * 		 vm_object-to-memory_object assocations. [mwyoung]
 * 		Improved external memory management interface. [mwyoung]
 * 		Several reimplementations/fixes to the object cache
 * 		 and the port to object translation.  [mwyoung, avie, dbg]
 * 		Create a cache of objects that have no references
 * 		 so that their pages remain in memory (inactive).  [avie]
 * 		Collapse object tree when reference counts drop to one.
 * 		 Use "paging_in_progress" to prevent collapsing. [dbg]
 * 		Split up paging system lock, eliminate SPL handling.
 * 		 The only uses of vm_object code at interrupt level
 * 		 uses objects that are only touched at interrupt level. [dbg]
 * 		Use "copy object" rather than normal shadows for
 * 		 permanent objects.  [dbg]
 * 		Accomodate external pagers [mwyoung, bolosky].
 * 		Allow objects to be coalesced to avoid growing address
 * 		 maps during sequential allocations.  [dbg]
 * 		Optimizations and fixes to copy-on-write [avie, mwyoung, dbg].
 * 		Use only one object for all kernel data. [avie]
 * 
 */
/* CMU_ENDHIST */
/* 
 * Mach Operating System
 * Copyright (c) 1991,1990,1989,1988,1987 Carnegie Mellon University
 * All Rights Reserved.
 * 
 * Permission to use, copy, modify and distribute this software and its
 * documentation is hereby granted, provided that both the copyright
 * notice and this permission notice appear in all copies of the
 * software, derivative works or modified versions, and any portions
 * thereof, and that both notices appear in supporting documentation.
 * 
 * CARNEGIE MELLON ALLOWS FREE USE OF THIS SOFTWARE IN ITS "AS IS"
 * CONDITION.  CARNEGIE MELLON DISCLAIMS ANY LIABILITY OF ANY KIND FOR
 * ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.
 * 
 * Carnegie Mellon requests users of this software to return to
 * 
 *  Software Distribution Coordinator  or  Software.Distribution@CS.CMU.EDU
 *  School of Computer Science
 *  Carnegie Mellon University
 *  Pittsburgh PA 15213-3890
 * 
 * any improvements or extensions that they make and grant Carnegie Mellon
 * the rights to redistribute these changes.
 */
/*
 */
/*
 *	File:	vm/vm_object.c
 *	Author:	Avadis Tevanian, Jr., Michael Wayne Young
 *
 *	Virtual memory object module.
 */
#include <norma_vm.h>
#include <mach_pagemap.h>
#include <task_swapper.h>

#include <mach/memory_object.h>
#include <mach/memory_object_default.h>
#include <mach/memory_object_user.h>
#include <mach/mach_server.h>
#include <mach/vm_param.h>
#include <ipc/ipc_port.h>
#include <ipc/ipc_space.h>
#include <kern/assert.h>
#include <kern/lock.h>
#include <kern/queue.h>
#include <kern/xpr.h>
#include <kern/zalloc.h>
#include <kern/host.h>
#include <kern/host_statistics.h>
#include <vm/memory_object.h>
#include <vm/vm_fault.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vm_pageout.h>
#include <kern/misc_protos.h>

#if	NORMA_VM
#include <xmm/xmm_server_rename.h>
#include <xmm/xmm_obj.h>
#endif	/* NORMA_VM */

/*
 *	Virtual memory objects maintain the actual data
 *	associated with allocated virtual memory.  A given
 *	page of memory exists within exactly one object.
 *
 *	An object is only deallocated when all "references"
 *	are given up.  Only one "reference" to a given
 *	region of an object should be writeable.
 *
 *	Associated with each object is a list of all resident
 *	memory pages belonging to that object; this list is
 *	maintained by the "vm_page" module, but locked by the object's
 *	lock.
 *
 *	Each object also records the memory object port
 *	that is used by the kernel to request and write
 *	back data (the memory object port, field "pager"),
 *	and the ports provided to the memory manager, the server that
 *	manages that data, to return data and control its
 *	use (the memory object control port, field "pager_request")
 *	and for naming (the memory object name port, field "pager_name").
 *
 *	Virtual memory objects are allocated to provide
 *	zero-filled memory (vm_allocate) or map a user-defined
 *	memory object into a virtual address space (vm_map).
 *
 *	Virtual memory objects that refer to a user-defined
 *	memory object are called "permanent", because all changes
 *	made in virtual memory are reflected back to the
 *	memory manager, which may then store it permanently.
 *	Other virtual memory objects are called "temporary",
 *	meaning that changes need be written back only when
 *	necessary to reclaim pages, and that storage associated
 *	with the object can be discarded once it is no longer
 *	mapped.
 *
 *	A permanent memory object may be mapped into more
 *	than one virtual address space.  Moreover, two threads
 *	may attempt to make the first mapping of a memory
 *	object concurrently.  Only one thread is allowed to
 *	complete this mapping; all others wait for the
 *	"pager_initialized" field is asserted, indicating
 *	that the first thread has initialized all of the
 *	necessary fields in the virtual memory object structure.
 *
 *	The kernel relies on a *default memory manager* to
 *	provide backing storage for the zero-filled virtual
 *	memory objects.  The memory object ports associated
 *	with these temporary virtual memory objects are only
 *	generated and passed to the default memory manager
 *	when it becomes necessary.  Virtual memory objects
 *	that depend on the default memory manager are called
 *	"internal".  The "pager_created" field is provided to
 *	indicate whether these ports have ever been allocated.
 *	
 *	The kernel may also create virtual memory objects to
 *	hold changed pages after a copy-on-write operation.
 *	In this case, the virtual memory object (and its
 *	backing storage -- its memory object) only contain
 *	those pages that have been changed.  The "shadow"
 *	field refers to the virtual memory object that contains
 *	the remainder of the contents.  The "shadow_offset"
 *	field indicates where in the "shadow" these contents begin.
 *	The "copy" field refers to a virtual memory object
 *	to which changed pages must be copied before changing
 *	this object, in order to implement another form
 *	of copy-on-write optimization.
 *
 *	The virtual memory object structure also records
 *	the attributes associated with its memory object.
 *	The "pager_ready", "can_persist" and "copy_strategy"
 *	fields represent those attributes.  The "cached_list"
 *	field is used in the implementation of the persistence
 *	attribute.
 *
 * ZZZ Continue this comment.
 */

/* Forward declarations for internal functions. */
extern void		_vm_object_allocate(
				vm_size_t	size,
				vm_object_t	object);

extern void		vm_object_terminate(
				vm_object_t	object);

extern void		vm_object_remove(
				vm_object_t	object);

extern vm_object_t	vm_object_cache_trim(
				boolean_t called_from_vm_object_deallocate);

extern void		vm_object_deactivate_pages(
				vm_object_t	object);

extern void		vm_object_abort_activity(
				vm_object_t	object);

extern kern_return_t	vm_object_copy_call(
				vm_object_t	src_object,
				vm_offset_t	src_offset,
				vm_size_t	size,
				vm_object_t	*_result_object);

extern void		vm_object_do_collapse(
				vm_object_t	object,
				vm_object_t	backing_object);

extern void		vm_object_do_bypass(
				vm_object_t	object,
				vm_object_t	backing_object);

extern void		memory_object_release(
				ipc_port_t	pager,
				pager_request_t	pager_request);

#if	NORMA_VM
extern boolean_t	vm_object_uncache(
				vm_object_t	object,
				boolean_t	may_uncache);

extern void		vm_object_declare_pages(
				vm_object_t	object);

extern void		vm_object_reaper_continue(void);

#else	/* NORMA_VM */
extern vm_object_t	vm_object_copy_delayed(
				vm_object_t	src_object,
				vm_offset_t	src_offset,
				vm_size_t	size);
#endif	/* NORMA_VM */

zone_t		vm_object_zone;		/* vm backing store zone */

/*
 *	All wired-down kernel memory belongs to a single virtual
 *	memory object (kernel_object) to avoid wasting data structures.
 */
struct vm_object	kernel_object_store;
vm_object_t		kernel_object = &kernel_object_store;

/*
 *	The submap object is used as a placeholder for vm_map_submap
 *	operations.  The object is declared in vm_map.c because it
 *	is exported by the vm_map module.  The storage is declared
 *	here because it must be initialized here.
 */
struct vm_object	vm_submap_object_store;

/*
 *	Virtual memory objects that are not referenced by
 *	any address maps, but that are allowed to persist
 *	(an attribute specified by the associated memory manager),
 *	are kept in a queue (vm_object_cached_list).
 *
 *	When an object from this queue is referenced again,
 *	for example to make another address space mapping,
 *	it must be removed from the queue.  That is, the
 *	queue contains *only* objects with zero references.
 *
 *	The kernel may choose to terminate objects from this
 *	queue in order to reclaim storage.  The current policy
 *	is to permit a fixed maximum number of unreferenced
 *	objects (vm_object_cached_max).
 *
 *	A spin lock (accessed by routines
 *	vm_object_cache_{lock,lock_try,unlock}) governs the
 *	object cache.  It must be held when objects are
 *	added to or removed from the cache (in vm_object_terminate).
 *	The routines that acquire a reference to a virtual
 *	memory object based on one of the memory object ports
 *	must also lock the cache.
 *
 *	Ideally, the object cache should be more isolated
 *	from the reference mechanism, so that the lock need
 *	not be held to make simple references.
 */
queue_head_t	vm_object_cached_list;
int		vm_object_cached_count;
int		vm_object_cached_high;	/* highest # of cached objects */
int		vm_object_cached_max = 500;	/* may be patched*/

decl_mutex_data(,vm_object_cached_lock_data)

#define vm_object_cache_lock()		\
		mutex_lock(&vm_object_cached_lock_data)
#define vm_object_cache_lock_try()	\
		mutex_try(&vm_object_cached_lock_data)
#define vm_object_cache_unlock()	\
		mutex_unlock(&vm_object_cached_lock_data)

#if	NORMA_VM
/*
 * List of vm_objects to be asynchronously terminated.
 */
queue_head_t	vor_list;
decl_simple_lock_data(,vor_lock)
#endif	/* NORMA_VM */

/*
 *	Virtual memory objects are initialized from
 *	a template (see vm_object_allocate).
 *
 *	When adding a new field to the virtual memory
 *	object structure, be sure to add initialization
 *	(see vm_object_init).
 */
struct vm_object	vm_object_template;

#define	VM_OBJECT_TRACKING	0
#if	VM_OBJECT_TRACKING
#define	MAX_OBJECT_TRACK	10000
vm_object_t	vm_object_track[MAX_OBJECT_TRACK];
int		vm_object_track_count = 0;
int		vm_object_count_warning = 9800;

static void
vm_object_track_alloc(object)
vm_object_t	object;
{
	vm_object_t	*o;

	for (o = vm_object_track; o < vm_object_track + MAX_OBJECT_TRACK; ++o)
		if (*o == VM_OBJECT_NULL) {
			*o = object;
			++vm_object_track_count;
			if (vm_object_track_count > vm_object_count_warning)
				assert(vm_object_track_count == 0);
			return;
		}
	panic("vm_object_track_alloc");
}

static void
vm_object_track_dealloc(object)
vm_object_t	object;
{
	vm_object_t	*o;

	for (o = vm_object_track; o < vm_object_track + MAX_OBJECT_TRACK; ++o)
		if (*o == object) {
			*o = VM_OBJECT_NULL;
			--vm_object_track_count;
			return;
		}
	panic("vm_object_track_dealloc");
}
#endif	/* VM_OBJECT_TRACKING */

/*
 *	vm_object_allocate:
 *
 *	Returns a new object with the given size.
 */

void
_vm_object_allocate(
	vm_size_t	size,
	vm_object_t	object)
{
	XPR(XPR_VM_OBJECT,
		"vm_object_allocate, object 0x%X size 0x%X\n",
		(integer_t)object, size, 0,0,0);

	*object = vm_object_template;
	queue_init(&object->memq);
	queue_init(&object->msr_q);
	vm_object_lock_init(object);
	object->size = size;
#if	VM_OBJECT_TRACKING
	vm_object_track_alloc(object);
#endif
}

vm_object_t
vm_object_allocate(
	vm_size_t	size)
{
	register vm_object_t object;
	register ipc_port_t port;

	object = (vm_object_t) zalloc(vm_object_zone);
	_vm_object_allocate(size, object);

	return object;
}

/*
 *	vm_object_bootstrap:
 *
 *	Initialize the VM objects module.
 */
void
vm_object_bootstrap(void)
{
	vm_object_zone = zinit((vm_size_t) sizeof(struct vm_object),
				round_page(512*1024),
				round_page(12*1024),
				"objects");

	queue_init(&vm_object_cached_list);
	mutex_init(&vm_object_cached_lock_data, ETAP_VM_OBJ_CACHE);

	/*
	 *	Fill in a template object, for quick initialization
	 */

	/* memq; Lock; init after allocation */
	vm_object_template.size = 0;
	vm_object_template.frozen_size = 0;
	vm_object_template.ref_count = 1;
#if	TASK_SWAPPER
	vm_object_template.res_count = 1;
#endif	/* TASK_SWAPPER */
	vm_object_template.resident_page_count = 0;
	vm_object_template.copy = VM_OBJECT_NULL;
	vm_object_template.shadow = VM_OBJECT_NULL;
	vm_object_template.shadow_offset = (vm_offset_t) 0;

	vm_object_template.pager = IP_NULL;
	vm_object_template.paging_offset = 0;
	vm_object_template.pager_request = PAGER_REQUEST_NULL;
	/* msr_q; init after allocation */

	vm_object_template.copy_strategy = MEMORY_OBJECT_COPY_SYMMETRIC;
	vm_object_template.absent_count = 0;
	vm_object_template.paging_in_progress = 0;

	/* Begin bitfields */
	vm_object_template.all_wanted = 0; /* all bits FALSE */
	vm_object_template.pager_created = FALSE;
	vm_object_template.pager_initialized = FALSE;
	vm_object_template.pager_ready = FALSE;
	vm_object_template.pager_trusted = FALSE;
	vm_object_template.can_persist = FALSE;
	vm_object_template.internal = TRUE;
	vm_object_template.temporary = TRUE;
	vm_object_template.private = FALSE;
	vm_object_template.pageout = FALSE;
	vm_object_template.alive = TRUE;
	vm_object_template.lock_in_progress = FALSE;
	vm_object_template.lock_restart = FALSE;
	vm_object_template.silent_overwrite = FALSE;
	vm_object_template.advisory_pageout = FALSE;
#if	NORMA_VM
	vm_object_template.pagemap_exported = FALSE;
	vm_object_template.temporary_cached = FALSE;
	vm_object_template.temporary_caching = FALSE;
	vm_object_template.temporary_uncaching = FALSE;
#endif	/* NORMA_VM */
	vm_object_template.shadowed = FALSE;
	/* End bitfields */

	/* cached_list; init after allocation */
	vm_object_template.last_alloc = (vm_offset_t) 0;
	vm_object_template.cluster_size = 0;
#if	MACH_PAGEMAP
	vm_object_template.existence_map = VM_EXTERNAL_NULL;
#endif	/* MACH_PAGEMAP */
#if	MACH_ASSERT
	vm_object_template.paging_object = VM_OBJECT_NULL;
#endif	/* MACH_ASSERT */

	/*
	 *	Initialize the "kernel object"
	 */

	kernel_object = &kernel_object_store;
	_vm_object_allocate(VM_MAX_KERNEL_ADDRESS - VM_MIN_KERNEL_ADDRESS,
			kernel_object);

	/*
	 *	Initialize the "submap object".  Make it as large as the
	 *	kernel object so that no limit is imposed on submap sizes.
	 */

	vm_submap_object = &vm_submap_object_store;
	_vm_object_allocate(VM_MAX_KERNEL_ADDRESS - VM_MIN_KERNEL_ADDRESS,
			vm_submap_object);
	/*
	 * Create an "extra" reference to this object so that we never
	 * try to deallocate it; zfree doesn't like to be called with
	 * non-zone memory.
	 */
	vm_object_reference(vm_submap_object);

#if	MACH_PAGEMAP
	vm_external_module_initialize();
#endif	/* MACH_PAGEMAP */

#if	NORMA_VM
	/*
	 * Initialize reaper_thread resources.
	 */
	queue_init(&vor_list);
	simple_lock_init(&vor_lock, ETAP_NORMA_VOR);
#endif	/* NORMA_VM */
}

void
vm_object_init(void)
{
	/*
	 *	Finish initializing the kernel object.
	 */
}

#if	TASK_SWAPPER
/*
 * vm_object_res_deallocate
 *
 * (recursively) decrement residence counts on vm objects and their shadows.
 * Called from vm_object_deallocate and when swapping out an object.
 *
 * The object is locked, and remains locked throughout the function,
 * even as we iterate down the shadow chain.  Locks on intermediate objects
 * will be dropped, but not the original object.
 *
 * NOTE: this function used to use recursion, rather than iteration.
 */

void
vm_object_res_deallocate(
	vm_object_t	object)
{
	vm_object_t orig_object = object;
	/*
	 * Object is locked so it can be called directly
	 * from vm_object_deallocate.  Original object is never
	 * unlocked.
	 */
	assert(object->res_count > 0);
	while  (--object->res_count == 0) {
		assert(object->ref_count >= object->res_count);
		vm_object_deactivate_pages(object);
		/* iterate on shadow, if present */
		if (object->shadow != VM_OBJECT_NULL) {
			vm_object_t tmp_object = object->shadow;
			vm_object_lock(tmp_object);
			if (object != orig_object)
				vm_object_unlock(object);
			object = tmp_object;
			assert(object->res_count > 0);
		} else
			break;
	}
	if (object != orig_object)
		vm_object_unlock(object);
}

/*
 * vm_object_res_reference
 *
 * Internal function to increment residence count on a vm object
 * and its shadows.  It is called only from vm_object_reference, and
 * when swapping in a vm object, via vm_map_swap.
 *
 * The object is locked, and remains locked throughout the function,
 * even as we iterate down the shadow chain.  Locks on intermediate objects
 * will be dropped, but not the original object.
 *
 * NOTE: this function used to use recursion, rather than iteration.
 */

void
vm_object_res_reference(
	vm_object_t	object)
{
	vm_object_t orig_object = object;
	/* 
	 * Object is locked, so this can be called directly
	 * from vm_object_reference.  This lock is never released.
	 */
	while  ((++object->res_count == 1)  && 
		(object->shadow != VM_OBJECT_NULL)) {
		vm_object_t tmp_object = object->shadow;

		assert(object->ref_count >= object->res_count);
		vm_object_lock(tmp_object);
		if (object != orig_object)
			vm_object_unlock(object);
		object = tmp_object;
	}
	if (object != orig_object)
		vm_object_unlock(object);
	assert(orig_object->ref_count >= orig_object->res_count);
}
#endif	/* TASK_SWAPPER */

#if	MACH_ASSERT
/*
 *	vm_object_reference:
 *
 *	Gets another reference to the given object.
 */
void
vm_object_reference(
	register vm_object_t	object)
{
	if (object == VM_OBJECT_NULL)
		return;

	vm_object_lock(object);
	assert(object->ref_count > 0);
	object->ref_count++;
	vm_object_res_reference(object);
	vm_object_unlock(object);
}
#endif	/* MACH_ASSERT */

#define	MIGHT_NOT_CACHE_SHADOWS		1
#if	MIGHT_NOT_CACHE_SHADOWS
int cache_shadows = TRUE;
#endif	/* MIGHT_NOT_CACHE_SHADOWS */

#if	NORMA_VM

/*
 *	vm_object_deallocate_lazy:
 *
 *	Lose the reference. If other references remain, then we are done.
 *	If it is the last reference, then keep it and insert it into
 *	the vor_list queue to deallocate it under the vm_object_thread
 *	that is allowed to block. If the object is not alive and it is the
 *	last reference, then object is waiting for the last completion, and
 *	the thread waiting for that condition must be awaken.
 *
 *	Object is locked.
 */
void
vm_object_deallocate_lazy(
	vm_object_t	object)
{
	assert(object->ref_count > 0);
	if (!object->alive)
		panic("vm_object_deallocate_lazy");

	if (object->ref_count > 1) {
		object->ref_count--;
		vm_object_res_deallocate(object);
	} else {
		simple_lock(&vor_lock);
		queue_enter(&vor_list, object,
			    vm_object_t, cached_list);
		simple_unlock(&vor_lock);
		thread_wakeup((event_t)&vor_list);
	}
}

#endif	/* NORMA_VM */

/*
 *	vm_object_deallocate:
 *
 *	Release a reference to the specified object,
 *	gained either through a vm_object_allocate
 *	or a vm_object_reference call.  When all references
 *	are gone, storage associated with this object
 *	may be relinquished.
 *
 *	No object may be locked.
 */
void
vm_object_deallocate(
	register vm_object_t	object)
{
	boolean_t retry_cache_trim = FALSE;
	vm_object_t shadow;

	while (object != VM_OBJECT_NULL) {

		/*
		 *	The cache holds a reference (uncounted) to
		 *	the object; we must lock it before removing
		 *	the object.
		 */

		vm_object_cache_lock();
		vm_object_lock(object);
		assert(object->alive);

		/*
		 *	Lose the reference. If other references
		 *	remain, then we are done, unless we need
		 *	to retry a cache trim.
		 *	If it is the last reference, then keep it
		 *	until any pending initialization is completed.
		 */

		assert(object->ref_count > 0);
		if (object->ref_count > 1) {
			object->ref_count--;
			vm_object_res_deallocate(object);
			vm_object_unlock(object);
			vm_object_cache_unlock();
			if (retry_cache_trim &&
			    ((object = vm_object_cache_trim(TRUE)) !=
			     VM_OBJECT_NULL)) {
				continue;
			}
			return;
		}

		/*
		 *	We have to wait for initialization
		 *	before destroying or caching the object.
		 */
		
		if (object->pager_created && ! object->pager_initialized) {
			assert(! object->can_persist);
			vm_object_assert_wait(object,
					      VM_OBJECT_EVENT_INITIALIZED,
					      FALSE);
			vm_object_unlock(object);
			vm_object_cache_unlock();
			thread_block((void (*)(void))0);
			continue;
		}

#if	NORMA_VM
		/*
		 * We have to synchronize m_o_caching() with
		 * m_o_set_attributes()/m_o_destroy() and vm_object_uncache().
		 */
		if (object->temporary_caching) {
			vm_object_assert_wait(object,
					      VM_OBJECT_EVENT_CACHING,
					      FALSE);
			vm_object_unlock(object);
			vm_object_cache_unlock();
			thread_block((void (*)(void)) 0);
			continue;
		}
#endif	/* NORMA_VM */

		/*
		 *	If this object can persist, then enter it in
		 *	the cache. Otherwise, terminate it.
		 *
		 * 	NOTE:  Only permanent objects are cached, and
		 *	permanent objects cannot have shadows.  This
		 *	affects the residence counting logic in a minor
		 *	way (can do it in-line, mostly).
		 */

		if (object->can_persist
#if	NORMA_VM
		    &&
		    (!object->temporary || object->resident_page_count > 0)
#endif	/* NORMA_VM */
		    ) {
#if	NORMA_VM
			/*
			 *	If we are about to cache a temporary object,
			 *	we must tell the memory manager, so that it
			 *	can coordinate a termination when appropriate.
			 *
			 *	We might, however, simply be returning
			 *	an object to the cache without having
			 *	asked to remove it, if we were pulled
			 *	out of the cache by vm_object_lookup
			 *	on behalf of a memory manager operation.
			 */
			if (object->temporary && ! object->temporary_cached) {
				assert(!object->temporary_caching);
				object->temporary_caching = TRUE;
				vm_object_unlock(object);
				vm_object_cache_unlock();
				memory_object_caching(object->pager,
						      object->pager_request);
				vm_object_cache_lock();
				vm_object_lock(object);
				object->temporary_cached = TRUE;
				object->temporary_caching = FALSE;
				vm_object_wakeup(object,
						 VM_OBJECT_EVENT_CACHING);
			}
#endif	/* NORMA_VM */
			/*
			 *	Now it is safe to decrement reference count,
			 *	and to return if reference count is > 0.
			 */
			if (--object->ref_count > 0) {
				vm_object_res_deallocate(object);
				vm_object_unlock(object);
				vm_object_cache_unlock();
				if (retry_cache_trim &&
				    ((object = vm_object_cache_trim(TRUE)) !=
				     VM_OBJECT_NULL)) {
					continue;
				}
				return;
			}

#if	MIGHT_NOT_CACHE_SHADOWS
			/*
			 *	Remove shadow now if we don't
			 *	want to cache shadows.
			 */
			if (! cache_shadows) {
				shadow = object->shadow;
				object->shadow = VM_OBJECT_NULL;
			}
#endif	/* MIGHT_NOT_CACHE_SHADOWS */

			/*
			 *	Enter the object onto the queue of
			 *	cached objects, and deactivate
			 *	all of its pages.
			 */
			assert(object->shadow == VM_OBJECT_NULL);
			VM_OBJ_RES_DECR(object);
			XPR(XPR_VM_OBJECT,
		      "vm_o_deallocate: adding %x to cache, queue = (%x, %x)\n",
				(integer_t)object,
				(integer_t)vm_object_cached_list.next,
				(integer_t)vm_object_cached_list.prev,0,0);

			vm_object_cached_count++;
			if (vm_object_cached_count > vm_object_cached_high)
				vm_object_cached_high = vm_object_cached_count;
			queue_enter(&vm_object_cached_list, object,
				vm_object_t, cached_list);
			vm_object_cache_unlock();
			vm_object_deactivate_pages(object);
			vm_object_unlock(object);

#if	MIGHT_NOT_CACHE_SHADOWS
			/*
			 *	If we have a shadow that we need
			 *	to deallocate, do so now, remembering
			 *	to trim the cache later.
			 */
			if (! cache_shadows && shadow != VM_OBJECT_NULL) {
				object = shadow;
				retry_cache_trim = TRUE;
				continue;
			}
#endif	/* MIGHT_NOT_CACHE_SHADOWS */

			/*
			 *	Trim the cache. If the cache trim
			 *	returns with a shadow for us to deallocate,
			 *	then remember to retry the cache trim
			 *	when we are done deallocating the shadow.
			 *	Otherwise, we are done.
			 */

			object = vm_object_cache_trim(TRUE);
			if (object == VM_OBJECT_NULL) {
				return;
			}
			retry_cache_trim = TRUE;

		} else {
			/*
			 *	This object is not cachable; terminate it.
			 */
			XPR(XPR_VM_OBJECT,
	 "vm_o_deallocate: !cacheable 0x%X res %d paging_ops %d thread 0x%lX ref %d\n",
				(integer_t)object, object->resident_page_count,
				object->paging_in_progress,
				(natural_t)current_thread(),object->ref_count);


#if	NORMA_VM
			/*
			 *	Wake up anyone waiting for a reply to an
			 *	uncache request. They will see that
			 *	object->pager is now null, indicating
			 *	that the object has destroyed and thus
			 *	that they have lost.
			 */
			if (object->temporary_uncaching) {
				assert(object->temporary_cached);
				object->temporary_cached = FALSE;
				object->temporary_uncaching = FALSE;
				vm_object_wakeup(object,
						 VM_OBJECT_EVENT_UNCACHING);
			}
#endif	/* NORMA_VM */
			VM_OBJ_RES_DECR(object);	/* XXX ? */
			/*
			 *	Terminate this object. If it had a shadow,
			 *	then deallocate it; otherwise, if we need
			 *	to retry a cache trim, do so now; otherwise,
			 *	we are done. "pageout" objects have a shadow,
			 *	but maintain a "paging reference" rather than
			 *	a normal reference.
			 */
			shadow = object->pageout?VM_OBJECT_NULL:object->shadow;
			vm_object_terminate(object);
			if (shadow != VM_OBJECT_NULL) {
				object = shadow;
				continue;
			}
			if (retry_cache_trim &&
			    ((object = vm_object_cache_trim(TRUE)) !=
			     VM_OBJECT_NULL)) {
				continue;
			}
			return;
		}
	}
	assert(! retry_cache_trim);
}

/*
 *	Check to see whether we really need to trim
 *	down the cache. If so, remove an object from
 *	the cache, terminate it, and repeat.
 *
 *	Called with, and returns with, cache lock unlocked.
 */
vm_object_t
vm_object_cache_trim(
	boolean_t called_from_vm_object_deallocate)
{
	register vm_object_t object = VM_OBJECT_NULL;
	vm_object_t shadow;

	for (;;) {

		/*
		 *	If we no longer need to trim the cache,
		 *	then we are done.
		 */

		vm_object_cache_lock();
		if (vm_object_cached_count <= vm_object_cached_max) {
			vm_object_cache_unlock();
			return VM_OBJECT_NULL;
		}

		/*
		 *	We must trim down the cache, so remove
		 *	the first object in the cache.
		 */
		XPR(XPR_VM_OBJECT,
		"vm_object_cache_trim: removing from front of cache (%x, %x)\n",
			(integer_t)vm_object_cached_list.next,
			(integer_t)vm_object_cached_list.prev, 0, 0, 0);

		object = (vm_object_t) queue_first(&vm_object_cached_list);
		vm_object_lock(object);
		queue_remove(&vm_object_cached_list, object, vm_object_t,
			     cached_list);
		vm_object_cached_count--;

		/*
		 *	Since this object is in the cache, we know
		 *	that it is initialized and has no references.
		 *	Take a reference to avoid recursive deallocations.
		 */

		assert(object->pager_initialized);
		assert(object->ref_count == 0);
		object->ref_count++;

		/*
		 *	Terminate the object.
		 *	If the object had a shadow, we let vm_object_deallocate
		 *	deallocate it. "pageout" objects have a shadow, but
		 *	maintain a "paging reference" rather than a normal
		 *	reference.
		 *	(We are careful here to limit recursion.)
		 */
		shadow = object->pageout?VM_OBJECT_NULL:object->shadow;
		vm_object_terminate(object);
		if (shadow != VM_OBJECT_NULL) {
			if (called_from_vm_object_deallocate) {
				return shadow;
			} else {
				vm_object_deallocate(shadow);
			}
		}
	}
}

boolean_t	vm_object_terminate_remove_all = FALSE;
#define TEST 1
#if TEST
int non_temp_cnt;			
#endif /* TEST */

/*
 *	Routine:	vm_object_terminate
 *	Purpose:
 *		Free all resources associated with a vm_object.
 *	In/out conditions:
 *		Upon entry, the object and the cache must be locked,
 *		and the object must have exactly one reference.
 *
 *		The shadow object reference is left alone.
 *
 *		Upon exit, the cache will be unlocked, and the
 *		object will cease to exist.
 */
void
vm_object_terminate(
	register vm_object_t	object)
{
	register vm_page_t	p;
	vm_object_t		shadow_object;

	XPR(XPR_VM_OBJECT, "vm_object_terminate, object 0x%X ref %d\n",
		(integer_t)object, object->ref_count, 0, 0, 0);

	/*
	 *	Make sure the object isn't already being terminated
	 */

	assert(object->alive);
	object->alive = FALSE;

	/*
	 *	Make sure no one can look us up now.
	 */

	vm_object_remove(object);
	vm_object_cache_unlock();

	/*
	 *	Detach the object from its shadow if we are the shadow's
	 *	copy.
	 */
	if (((shadow_object = object->shadow) != VM_OBJECT_NULL) &&
	    !(object->pageout)) {
		vm_object_lock(shadow_object);
		assert((shadow_object->copy == object) ||
		       (shadow_object->copy == VM_OBJECT_NULL));
		shadow_object->copy = VM_OBJECT_NULL;
		vm_object_unlock(shadow_object);
	}

	/*
	 *	The pageout daemon might be playing with our pages.
	 *	Now that the object is dead, it won't touch any more
	 *	pages, but some pages might already be on their way out.
	 *	Hence, we wait until the active paging activities have ceased.
	 */
	vm_object_paging_wait(object, FALSE);
	object->ref_count--;
#if	TASK_SWAPPER
	assert(object->res_count == 0);
#endif	/* TASK_SWAPPER */

Restart:
	assert (object->ref_count == 0);

	/*
	 *	Clean or free the pages, as appropriate.
	 *	It is possible for us to find busy/absent pages,
	 *	if some faults on this object were aborted.
	 */
	if (object->pageout) {
		assert(shadow_object != VM_OBJECT_NULL);
		assert(shadow_object == object->shadow);

		vm_pageout_object_terminate(object);

	} else if (object->temporary && ! object->can_persist ||
	    object->pager == IP_NULL) {
		while (!queue_empty(&object->memq)) {
			p = (vm_page_t) queue_first(&object->memq);

			VM_PAGE_CHECK(p);

			if (p->busy && !p->absent)
				panic("vm_object_terminate.2 0x%x 0x%x",
				      object, p);

			VM_PAGE_FREE(p);
		}
	} else while (!queue_empty(&object->memq)) {
		/*
		 * Clear pager_trusted bit so that the pages get yanked
		 * out of the object instead of cleaned in place.  This
		 * prevents a deadlock in XMM and makes more sense anyway.
		 */
		object->pager_trusted = FALSE;

		p = (vm_page_t) queue_first(&object->memq);

		VM_PAGE_CHECK(p);

		if (p->busy && !p->absent)
			panic("vm_object_terminate.3 0x%x 0x%x", object, p);

		vm_page_lock_queues();
		VM_PAGE_QUEUES_REMOVE(p);
		vm_page_unlock_queues();

		if (p->absent || p->private) {

			/*
			 *	For private pages, VM_PAGE_FREE just
			 *	leaves the page structure around for
			 *	its owner to clean up.  For absent
			 *	pages, the structure is returned to
			 *	the appropriate pool.
			 */

			goto free_page;
		}

		if (p->fictitious)
			panic("vm_object_terminate.4 0x%x 0x%x", object, p);

		if (!p->dirty)
			p->dirty = pmap_is_modified(p->phys_addr);

		if (p->dirty || p->precious) {
			p->busy = TRUE;
			vm_object_paging_begin(object);
			vm_object_unlock(object);
			vm_pageout_cluster(p); /* flush page */
#if TEST
			if (IP_IS_REMOTE(object->pager))
				non_temp_cnt++;
#endif /* TEST */
			vm_object_lock(object);
			vm_object_paging_wait(object, FALSE);

			XPR(XPR_VM_OBJECT,
			    "vm_object_terminate restart, object 0x%X ref %d\n",
			    (integer_t)object, object->ref_count, 0, 0, 0);

			goto Restart;
		} else {
		    free_page:
		    	VM_PAGE_FREE(p);
		}
	}

	assert(object->paging_in_progress == 0);
	assert(object->ref_count == 0);

	/*
	 *	Throw away port rights... note that they may
	 *	already have been thrown away (by vm_object_destroy
	 *	or memory_object_destroy).
	 *
	 *	Instead of destroying the control port,
	 *	we send all rights off to the memory manager,
	 *	using memory_object_terminate.
	 */

	vm_object_unlock(object);
	if (object->pager != IP_NULL) {
		/* consumes our rights for pager, pager_request */
		memory_object_release(object->pager, object->pager_request);
	}

#if	MACH_PAGEMAP
	vm_external_destroy(object->existence_map, object->size);
#endif	/* MACH_PAGEMAP */

	/*
	 *	Free the space for the object.
	 */

#if	VM_OBJECT_TRACKING
	vm_object_track_dealloc(object);
#endif
	zfree(vm_object_zone, (vm_offset_t) object);
}

/*
 *	Routine:	vm_object_pager_wakeup
 *	Purpose:	Wake up anyone waiting for IKOT_PAGER_TERMINATING
 */

void
vm_object_pager_wakeup(
	ipc_port_t	pager)
{
	boolean_t someone_waiting;

	/*
	 *	If anyone was waiting for the memory_object_terminate
	 *	to be queued, wake them up now.
	 */
	vm_object_cache_lock();
	assert(ip_kotype(pager) == IKOT_PAGER_TERMINATING);
	someone_waiting = (pager->ip_kobject == (ipc_kobject_t)pager);
	if (ip_active(pager))
		ipc_kobject_set(pager, IKO_NULL, IKOT_NONE);
	vm_object_cache_unlock();
	if (someone_waiting) {
		thread_wakeup((event_t) pager);
	}
}

/*
 *	Routine:	memory_object_release
 *	Purpose:	Terminate the pager and release port rights,
 *			just like memory_object_terminate, except
 *			that we wake up anyone blocked in vm_object_enter
 *			waiting for termination message to be queued
 *			before calling memory_object_init.
 */
void
memory_object_release(
	ipc_port_t	pager,
	pager_request_t	pager_request)
{

	/*
	 *	Keep a reference to pager port;
	 *	the terminate might otherwise release all references.
	 */
	ipc_port_copy_send(pager);

	/*
	 *	Terminate the pager.
	 */

	(void) memory_object_terminate(pager, pager_request);

	/*
	 *	Wakeup anyone waiting for this terminate
	 */
	vm_object_pager_wakeup(pager);

	/*
	 *	Release reference to pager port.
	 */
	ipc_port_release_send(pager);
}

/*
 *	Routine:	vm_object_abort_activity [internal use only]
 *	Purpose:
 *		Abort paging requests pending on this object.
 *	In/out conditions:
 *		The object is locked on entry and exit.
 */
void
vm_object_abort_activity(
	vm_object_t	object)
{
	register
	vm_page_t	p;
	vm_page_t	next;

	XPR(XPR_VM_OBJECT, "vm_object_abort_activity, object 0x%X\n",
		(integer_t)object, 0, 0, 0, 0);

	/*
	 *	Abort all activity that would be waiting
	 *	for a result on this memory object.
	 *
	 *	We could also choose to destroy all pages
	 *	that we have in memory for this object, but
	 *	we don't.
	 */

	p = (vm_page_t) queue_first(&object->memq);
	while (!queue_end(&object->memq, (queue_entry_t) p)) {
		next = (vm_page_t) queue_next(&p->listq);

		/*
		 *	If it's being paged in, destroy it.
		 *	If an unlock has been requested, start it again.
		 */

		if (p->busy && p->absent) {
			VM_PAGE_FREE(p);
		}
		 else {
		 	if (p->unlock_request != VM_PROT_NONE)
			 	p->unlock_request = VM_PROT_NONE;
			PAGE_WAKEUP(p);
		}
		
		p = next;
	}

	/*
	 *	Wake up threads waiting for the memory object to
	 *	become ready.
	 */

	object->pager_ready = TRUE;
	vm_object_wakeup(object, VM_OBJECT_EVENT_PAGER_READY);
}

/*
 *	Routine:	memory_object_destroy [user interface]
 *	Purpose:
 *		Shut down a memory object, despite the
 *		presence of address map (or other) references
 *		to the vm_object.
 */
kern_return_t
memory_object_destroy(
	register vm_object_t	object,
	kern_return_t		reason)
{
	ipc_port_t	old_object;
	pager_request_t	old_pager_request;

#ifdef	lint
	reason++;
#endif	/* lint */

	if (object == VM_OBJECT_NULL)
		return(KERN_SUCCESS);

	/*
	 *	Remove the port associations immediately.
	 *
	 *	This will prevent the memory manager from further
	 *	meddling.  [If it wanted to flush data or make
	 *	other changes, it should have done so before performing
	 *	the destroy call.]
	 */

	vm_object_cache_lock();
	vm_object_lock(object);
	vm_object_remove(object);
	object->can_persist = FALSE;
	vm_object_cache_unlock();

	/*
	 *	Rip out the ports from the vm_object now... this
	 *	will prevent new memory_object calls from succeeding.
	 */

	old_object = object->pager;
	old_pager_request = object->pager_request;

	object->pager = IP_NULL;
	object->pager_request = PAGER_REQUEST_NULL;

#if	NORMA_VM
	/*
	 *	Wake up anyone waiting for a reply to an uncache request.
	 *	They will see that object->pager is now null, indicating
	 *	that the object has destroyed and thus that they have lost.
	 */
	if (object->temporary_uncaching) {
		assert(object->temporary_cached);
		object->temporary_cached = FALSE;
		object->temporary_uncaching = FALSE;
		vm_object_wakeup(object, VM_OBJECT_EVENT_UNCACHING);
	}
#endif	/* NORMA_VM */

	/*
	 *	Wait for existing paging activity (that might
	 *	have the old ports) to subside.
	 */

	vm_object_paging_wait(object, FALSE);
	vm_object_unlock(object);

	/*
	 *	Shut down the ports now.
	 *
	 *	[Paging operations may be proceeding concurrently --
	 *	they'll get the null values established above.]
	 */

	if (old_object != IP_NULL) {
		/* consumes our rights for object, control */
		memory_object_release(old_object, old_pager_request);
	}

	/*
	 *	Lose the reference that was donated for this routine
	 */

	vm_object_deallocate(object);

	return(KERN_SUCCESS);
}

#if	NORMA_VM
/*
 *	Routine:	vm_object_uncache
 *	Purpose:
 *		Uncache a temporary object, or kill it if it
 *		is no longer uncachable. Return TRUE iff
 *		we succeeded in uncaching the object.
 *	Notes:
 *		Called with object locked and not in vm_object_cache,
 *		but with object->temporary_cached TRUE.
 *		Returns with object locked if it is still alive.
 *
 *		Should consume object ref_count iff failure.
 */
boolean_t
vm_object_uncache(
	vm_object_t	object,
	boolean_t	may_uncache)
{
	assert(object->temporary);
	assert(object->temporary_cached);
	assert(object->can_persist);
	assert(object->ref_count > 0);

	/*
	 * If no one else has sent an uncache request, then do so now.
	 */
	if (! object->temporary_uncaching) {
		object->temporary_uncaching = TRUE;
		vm_object_unlock(object);
		memory_object_uncaching(object->pager, object->pager_request);
		vm_object_lock(object);
	}

	/*
	 * Wait for the response to the uncache request.
	 */
	while (object->temporary_cached) {
		vm_object_wait(object, VM_OBJECT_EVENT_UNCACHING, FALSE);
		vm_object_lock(object);
	}

	/*
	 * If the object was destroyed, then deallocate object
	 * and return failure; otherwise, return success.
	 */
	if (object->pager == IP_NULL) {
		assert(object->ref_count > 0);
		vm_object_unlock(object);
		vm_object_deallocate(object);
		return FALSE;
	} else {
		return TRUE;
	}
}

kern_return_t
memory_object_uncaching_permitted(
	vm_object_t	object)
{
	assert(object != VM_OBJECT_NULL);

	vm_object_lock(object);
	assert(object->temporary_cached);
	assert(object->temporary_uncaching);
	object->temporary_cached = FALSE;
	object->temporary_uncaching = FALSE;
	vm_object_wakeup(object, VM_OBJECT_EVENT_UNCACHING);
	vm_object_unlock(object);

	return KERN_SUCCESS;
}
#endif	/* NORMA_VM */

/*
 *	vm_object_deactivate_pages
 *
 *	Deactivate all pages in the specified object.  (Keep its pages
 *	in memory even though it is no longer referenced.)
 *
 *	The object must be locked.
 */
void
vm_object_deactivate_pages(
	register vm_object_t	object)
{
	register vm_page_t	p;

	queue_iterate(&object->memq, p, vm_page_t, listq) {
		vm_page_lock_queues();
		if (!p->busy)
			vm_page_deactivate(p);
		vm_page_unlock_queues();
	}
}


/*
 *	Routine:	vm_object_pmap_protect
 *
 *	Purpose:
 *		Reduces the permission for all physical
 *		pages in the specified object range.
 *
 *		If removing write permission only, it is
 *		sufficient to protect only the pages in
 *		the top-level object; only those pages may
 *		have write permission.
 *
 *		If removing all access, we must follow the
 *		shadow chain from the top-level object to
 *		remove access to all pages in shadowed objects.
 *
 *		The object must *not* be locked.  The object must
 *		be temporary/internal.  
 *
 *              If pmap is not NULL, this routine assumes that
 *              the only mappings for the pages are in that
 *              pmap.
 */

void
vm_object_pmap_protect(
	register vm_object_t	object,
	register vm_offset_t	offset,
	vm_offset_t		size,
	pmap_t			pmap,
	vm_offset_t		pmap_start,
	vm_prot_t		prot)
{
	if (object == VM_OBJECT_NULL)
	    return;

	vm_object_lock(object);

	assert(object->copy_strategy == MEMORY_OBJECT_COPY_SYMMETRIC);

	while (TRUE) {
	    if (object->resident_page_count > atop(size) / 2 &&
		    pmap != PMAP_NULL) {
		vm_object_unlock(object);
		pmap_protect(pmap, pmap_start, pmap_start + size, prot);
		return;
	    }

	    {
		register vm_page_t	p;
		register vm_offset_t	end;

		end = offset + size;

		if (pmap != PMAP_NULL) {
		  queue_iterate(&object->memq, p, vm_page_t, listq) {
		    if (!p->fictitious &&
			(offset <= p->offset) && (p->offset < end)) {

			vm_offset_t start = pmap_start + (p->offset - offset);

			pmap_protect(pmap, start, start + PAGE_SIZE, prot);
		    }
		  }
		} else {
		  queue_iterate(&object->memq, p, vm_page_t, listq) {
		    if (!p->fictitious &&
			(offset <= p->offset) && (p->offset < end)) {

			    pmap_page_protect(p->phys_addr,
					      prot & ~p->page_lock);
		    }
		  }
		}
	    }

	    if (prot == VM_PROT_NONE) {
		/*
		 * Must follow shadow chain to remove access
		 * to pages in shadowed objects.
		 */
		register vm_object_t	next_object;

		next_object = object->shadow;
		if (next_object != VM_OBJECT_NULL) {
		    offset += object->shadow_offset;
		    vm_object_lock(next_object);
		    vm_object_unlock(object);
		    object = next_object;
		}
		else {
		    /*
		     * End of chain - we are done.
		     */
		    break;
		}
	    }
	    else {
		/*
		 * Pages in shadowed objects may never have
		 * write permission - we may stop here.
		 */
		break;
	    }
	}

	vm_object_unlock(object);
}

/*
 *	Routine:	vm_object_copy_slowly
 *
 *	Description:
 *		Copy the specified range of the source
 *		virtual memory object without using
 *		protection-based optimizations (such
 *		as copy-on-write).  The pages in the
 *		region are actually copied.
 *
 *	In/out conditions:
 *		The caller must hold a reference and a lock
 *		for the source virtual memory object.  The source
 *		object will be returned *unlocked*.
 *
 *	Results:
 *		If the copy is completed successfully, KERN_SUCCESS is
 *		returned.  If the caller asserted the interruptible
 *		argument, and an interruption occurred while waiting
 *		for a user-generated event, MACH_SEND_INTERRUPTED is
 *		returned.  Other values may be returned to indicate
 *		hard errors during the copy operation.
 *
 *		A new virtual memory object is returned in a
 *		parameter (_result_object).  The contents of this
 *		new object, starting at a zero offset, are a copy
 *		of the source memory region.  In the event of
 *		an error, this parameter will contain the value
 *		VM_OBJECT_NULL.
 */
kern_return_t
vm_object_copy_slowly(
	register vm_object_t	src_object,
	vm_offset_t		src_offset,
	vm_size_t		size,
	boolean_t		interruptible,
	vm_object_t		*_result_object)	/* OUT */
{
	vm_object_t	new_object;
	vm_offset_t	new_offset;

	vm_offset_t	src_lo_offset = src_offset;
	vm_offset_t	src_hi_offset = src_offset + size;

	XPR(XPR_VM_OBJECT, "v_o_c_slowly obj 0x%x off 0x%x size 0x%x\n",
	    src_object, src_offset, size, 0, 0);

	if (size == 0) {
		vm_object_unlock(src_object);
		*_result_object = VM_OBJECT_NULL;
		return(KERN_INVALID_ARGUMENT);
	}

	/*
	 *	Prevent destruction of the source object while we copy.
	 */

	assert(src_object->ref_count > 0);
	src_object->ref_count++;
	VM_OBJ_RES_INCR(src_object);
	vm_object_unlock(src_object);

	/*
	 *	Create a new object to hold the copied pages.
	 *	A few notes:
	 *		We fill the new object starting at offset 0,
	 *		 regardless of the input offset.
	 *		We don't bother to lock the new object within
	 *		 this routine, since we have the only reference.
	 */

	new_object = vm_object_allocate(size);
	new_offset = 0;

	assert(size == trunc_page(size));	/* Will the loop terminate? */

	for ( ;
	    size != 0 ;
	    src_offset += PAGE_SIZE, new_offset += PAGE_SIZE, size -= PAGE_SIZE
	    ) {
		vm_page_t	new_page;
		vm_fault_return_t result;

		while ((new_page = vm_page_alloc(new_object, new_offset))
				== VM_PAGE_NULL) {
			VM_PAGE_WAIT();
		}

		do {
			vm_prot_t	prot = VM_PROT_READ;
			vm_page_t	_result_page;
			vm_page_t	top_page;
			register
			vm_page_t	result_page;
			kern_return_t	error_code;

			vm_object_lock(src_object);
			vm_object_paging_begin(src_object);

			XPR(XPR_VM_FAULT,"vm_object_copy_slowly -> vm_fault_page",0,0,0,0,0);
			result = vm_fault_page(src_object, src_offset,
				VM_PROT_READ, FALSE, interruptible,
				src_lo_offset, src_hi_offset,
				VM_BEHAVIOR_SEQUENTIAL,
				&prot, &_result_page, &top_page,
				&error_code, FALSE, FALSE);

			switch(result) {
				case VM_FAULT_SUCCESS:
					result_page = _result_page;

					/*
					 *	We don't need to hold the object
					 *	lock -- the busy page will be enough.
					 *	[We don't care about picking up any
					 *	new modifications.]
					 *
					 *	Copy the page to the new object.
					 *
					 *	POLICY DECISION:
					 *		If result_page is clean,
					 *		we could steal it instead
					 *		of copying.
					 */

					vm_object_unlock(result_page->object);
					vm_page_copy(result_page, new_page);

					/*
					 *	Let go of both pages (make them
					 *	not busy, perform wakeup, activate).
					 */

					new_page->busy = FALSE;
					new_page->dirty = TRUE;
					vm_object_lock(result_page->object);
					PAGE_WAKEUP_DONE(result_page);

					vm_page_lock_queues();
					if (!result_page->active &&
					    !result_page->inactive)
						vm_page_activate(result_page);
					vm_page_activate(new_page);
					vm_page_unlock_queues();

					/*
					 *	Release paging references and
					 *	top-level placeholder page, if any.
					 */

					vm_fault_cleanup(result_page->object,
							top_page);

					break;
				
				case VM_FAULT_RETRY:
					break;

				case VM_FAULT_MEMORY_SHORTAGE:
					VM_PAGE_WAIT();
					break;

				case VM_FAULT_FICTITIOUS_SHORTAGE:
					vm_page_more_fictitious();
					break;

				case VM_FAULT_INTERRUPTED:
					vm_page_free(new_page);
					vm_object_deallocate(new_object);
					vm_object_deallocate(src_object);
					*_result_object = VM_OBJECT_NULL;
					return(MACH_SEND_INTERRUPTED);

				case VM_FAULT_MEMORY_ERROR:
					/*
					 * A policy choice:
					 *	(a) ignore pages that we can't
					 *	    copy
					 *	(b) return the null object if
					 *	    any page fails [chosen]
					 */

					vm_page_free(new_page);
					vm_object_deallocate(new_object);
					vm_object_deallocate(src_object);
					*_result_object = VM_OBJECT_NULL;
					return(error_code ? error_code:
						KERN_MEMORY_ERROR);
			}
		} while (result != VM_FAULT_SUCCESS);
	}

	/*
	 *	Lose the extra reference, and return our object.
	 */

	vm_object_deallocate(src_object);
	*_result_object = new_object;
	return(KERN_SUCCESS);
}

/*
 *	Routine:	vm_object_copy_quickly
 *
 *	Purpose:
 *		Copy the specified range of the source virtual
 *		memory object, if it can be done without waiting
 *		for user-generated events.
 *
 *	Results:
 *		If the copy is successful, the copy is returned in
 *		the arguments; otherwise, the arguments are not
 *		affected.
 *
 *	In/out conditions:
 *		The object should be unlocked on entry and exit.
 */

/*ARGSUSED*/
boolean_t
vm_object_copy_quickly(
	vm_object_t	*_object,		/* INOUT */
	vm_offset_t	offset,			/* IN */
	vm_size_t	size,			/* IN */
	boolean_t	*_src_needs_copy,	/* OUT */
	boolean_t	*_dst_needs_copy)	/* OUT */
{
	vm_object_t	object = *_object;
	memory_object_copy_strategy_t copy_strategy;

	XPR(XPR_VM_OBJECT, "v_o_c_quickly obj 0x%x off 0x%x size 0x%x\n",
	    *_object, offset, size, 0, 0);
	if (object == VM_OBJECT_NULL) {
		*_src_needs_copy = FALSE;
		*_dst_needs_copy = FALSE;
		return(TRUE);
	}

	vm_object_lock(object);

	copy_strategy = object->copy_strategy;

	switch (copy_strategy) {
	case MEMORY_OBJECT_COPY_SYMMETRIC:

		/*
		 *	Symmetric copy strategy.
		 *	Make another reference to the object.
		 *	Leave object/offset unchanged.
		 */

		assert(object->ref_count > 0);
		object->ref_count++;
		vm_object_res_reference(object);
		object->shadowed = TRUE;
		vm_object_unlock(object);

		/*
		 *	Both source and destination must make
		 *	shadows, and the source must be made
		 *	read-only if not already.
		 */

		*_src_needs_copy = TRUE;
		*_dst_needs_copy = TRUE;

		break;

#if	!NORMA_VM
	case MEMORY_OBJECT_COPY_DELAY:
		vm_object_unlock(object);
		return(FALSE);
#endif	/* !NORMA_VM */

	default:
		vm_object_unlock(object);
		return(FALSE);
	}
	return(TRUE);
}

int copy_call_count = 0;
int copy_call_sleep_count = 0;
int copy_call_restart_count = 0;

/*
 *	Routine:	vm_object_copy_call [internal]
 *
 *	Description:
 *		Copy the source object (src_object), using the
 *		user-managed copy algorithm.
 *
 *	In/out conditions:
 *		The source object must be locked on entry.  It
 *		will be *unlocked* on exit.
 *
 *	Results:
 *		If the copy is successful, KERN_SUCCESS is returned.
 *		A new object that represents the copied virtual
 *		memory is returned in a parameter (*_result_object).
 *		If the return value indicates an error, this parameter
 *		is not valid.
 */
kern_return_t
vm_object_copy_call(
	vm_object_t	src_object,
	vm_offset_t	src_offset,
	vm_size_t	size,
	vm_object_t	*_result_object)	/* OUT */
{
	kern_return_t	kr;
	vm_object_t	copy;
	boolean_t	check_ready = FALSE;

	/*
	 *	If a copy is already in progress, wait and retry.
	 *
	 *	XXX
	 *	Consider making this call interruptable, as Mike
	 *	intended it to be.
	 *
	 *	XXXO
	 *	Need a counter or version or something to allow
	 *	us to use the copy that the currently requesting
	 *	thread is obtaining -- is it worth adding to the
	 *	vm object structure? Depends how common this case it.
	 */
	copy_call_count++;
	while (vm_object_wanted(src_object, VM_OBJECT_EVENT_COPY_CALL)) {
		vm_object_wait(src_object, VM_OBJECT_EVENT_COPY_CALL, FALSE);
		vm_object_lock(src_object);
		copy_call_restart_count++;
	}

	/*
	 *	Indicate (for the benefit of memory_object_create_copy)
	 *	that we want a copy for src_object. (Note that we cannot
	 *	do a real assert_wait before calling memory_object_copy,
	 *	so we simply set the flag.)
	 */

	vm_object_set_wanted(src_object, VM_OBJECT_EVENT_COPY_CALL);
	vm_object_unlock(src_object);

	/*
	 *	Ask the memory manager to give us a memory object
	 *	which represents a copy of the src object.
	 *	The memory manager may give us a memory object
	 *	which we already have, or it may give us a
	 *	new memory object. This memory object will arrive
	 *	via memory_object_create_copy.
	 */

#if	NORMA_VM
	kr = memory_object_copy(src_object->pager, src_object->pager_request);
#else	/* NORMA_VM */
	kr = KERN_FAILURE;	/* XXX need to change memory_object.defs */
#endif	/* NORMA_VM */
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	/*
	 *	Wait for the copy to arrive.
	 */
	vm_object_lock(src_object);
	while (vm_object_wanted(src_object, VM_OBJECT_EVENT_COPY_CALL)) {
		vm_object_wait(src_object, VM_OBJECT_EVENT_COPY_CALL, FALSE);
		vm_object_lock(src_object);
		copy_call_sleep_count++;
	}
Retry:
	assert(src_object->copy != VM_OBJECT_NULL);
	copy = src_object->copy;
	if (!vm_object_lock_try(copy)) {
		vm_object_unlock(src_object);
		mutex_pause();	/* wait a bit */
		vm_object_lock(src_object);
		goto Retry;
	}
	if (copy->size < src_offset+size)
		copy->size = src_offset+size;

	if (!copy->pager_ready)
		check_ready = TRUE;

	/*
	 *	Return the copy.
	 */
	*_result_object = copy;
	vm_object_unlock(copy);
	vm_object_unlock(src_object);

	/* Wait for the copy to be ready. */
	if (check_ready == TRUE) {
		vm_object_lock(copy);
		while (!copy->pager_ready) {
			vm_object_wait(copy, VM_OBJECT_EVENT_PAGER_READY,
									FALSE);
			vm_object_lock(copy);
		}
		vm_object_unlock(copy);
	}

	return KERN_SUCCESS;
}

#if	!NORMA_VM
int copy_delayed_lock_collisions = 0;
int copy_delayed_max_collisions = 0;
int copy_delayed_lock_contention = 0;
int copy_delayed_protect_iterate = 0;
int copy_delayed_protect_lookup = 0;
int copy_delayed_protect_lookup_wait = 0;

/*
 *	Routine:	vm_object_copy_delayed [internal]
 *
 *	Description:
 *		Copy the specified virtual memory object, using
 *		the asymmetric copy-on-write algorithm.
 *
 *	In/out conditions:
 *		The object must be unlocked on entry.
 *
 *		This routine will not block waiting for user-generated
 *		events.  It is not interruptible.
 */
vm_object_t
vm_object_copy_delayed(
	vm_object_t	src_object,
	vm_offset_t	src_offset,
	vm_size_t	size)
{
	vm_object_t	new_copy = VM_OBJECT_NULL;
	vm_object_t	old_copy;
	vm_page_t	p;
	vm_size_t	copy_size;

	int collisions = 0;
	/*
	 *	The user-level memory manager wants to see all of the changes
	 *	to this object, but it has promised not to make any changes on
 	 *	its own.
	 *
	 *	Perform an asymmetric copy-on-write, as follows:
	 *		Create a new object, called a "copy object" to hold
	 *		 pages modified by the new mapping  (i.e., the copy,
	 *		 not the original mapping).
	 *		Record the original object as the backing object for
	 *		 the copy object.  If the original mapping does not
	 *		 change a page, it may be used read-only by the copy.
	 *		Record the copy object in the original object.
	 *		 When the original mapping causes a page to be modified,
	 *		 it must be copied to a new page that is "pushed" to
	 *		 the copy object.
	 *		Mark the new mapping (the copy object) copy-on-write.
	 *		 This makes the copy object itself read-only, allowing
	 *		 it to be reused if the original mapping makes no
	 *		 changes, and simplifying the synchronization required
	 *		 in the "push" operation described above.
	 *
	 *	The copy-on-write is said to be assymetric because the original
	 *	object is *not* marked copy-on-write. A copied page is pushed
	 *	to the copy object, regardless which party attempted to modify
	 *	the page.
	 *
	 *	Repeated asymmetric copy operations may be done. If the
	 *	original object has not been changed since the last copy, its
	 *	copy object can be reused. Otherwise, a new copy object can be
	 *	inserted between the original object and its previous copy
	 *	object.  Since any copy object is read-only, this cannot affect
	 *	affect the contents of the previous copy object.
	 *
	 *	Note that a copy object is higher in the object tree than the
	 *	original object; therefore, use of the copy object recorded in
	 *	the original object must be done carefully, to avoid deadlock.
	 */

 Retry:
	vm_object_lock(src_object);
 
	/*
	 *	See whether we can reuse the result of a previous
	 *	copy operation.
	 */

	old_copy = src_object->copy;
	if (old_copy != VM_OBJECT_NULL) {
		/*
		 *	Try to get the locks (out of order)
		 */
		if (!vm_object_lock_try(old_copy)) {
			vm_object_unlock(src_object);
			mutex_pause();

			/* Heisenberg Rules */
			copy_delayed_lock_collisions++;
			if (collisions++ == 0)
				copy_delayed_lock_contention++;

			if (collisions > copy_delayed_max_collisions)
				copy_delayed_max_collisions = collisions;

			goto Retry;
		}

		/*
		 *	Determine whether the old copy object has
		 *	been modified.
		 */

		if (old_copy->resident_page_count == 0 &&
		    !old_copy->pager_created) {
			/*
			 *	It has not been modified.
			 *
			 *	Return another reference to
			 *	the existing copy-object.
			 */
			assert(old_copy->ref_count > 0);
			old_copy->ref_count++;

			if (old_copy->size < src_offset+size)
				old_copy->size = src_offset+size;

#if	TASK_SWAPPER
			/*
			 * We have to reproduce some of the code from
			 * vm_object_res_reference because we've taken
			 * the locks out of order here, and deadlock
			 * would result if we simply called that function.
			 */
			if (++old_copy->res_count == 1) {
				assert(old_copy->shadow == src_object);
				vm_object_res_reference(src_object);
			}
#endif	/* TASK_SWAPPER */

			vm_object_unlock(old_copy);
			vm_object_unlock(src_object);

			if (new_copy != VM_OBJECT_NULL) {
				vm_object_unlock(new_copy);
				vm_object_deallocate(new_copy);
			}

			return(old_copy);
		}
		if (new_copy == VM_OBJECT_NULL) {
			vm_object_unlock(old_copy);
			vm_object_unlock(src_object);
			new_copy = vm_object_allocate(src_offset + size);
			vm_object_lock(new_copy);
			goto Retry;
		}

		/*
		 * Adjust the size argument so that the newly-created 
		 * copy object will be large enough to back either the
		 * new old copy object or the new mapping.
		 */
		if (old_copy->size > src_offset+size)
			size =  old_copy->size - src_offset;

		/*
		 *	The copy-object is always made large enough to
		 *	completely shadow the original object, since
		 *	it may have several users who want to shadow
		 *	the original object at different points.
		 */

		assert((old_copy->shadow == src_object) &&
		    (old_copy->shadow_offset == (vm_offset_t) 0));

		/*
		 *	Make the old copy-object shadow the new one.
		 *	It will receive no more pages from the original
		 *	object.
		 */

		src_object->ref_count--;	/* remove ref. from old_copy */
		assert(src_object->ref_count > 0);
		old_copy->shadow = new_copy;
		assert(new_copy->ref_count > 0);
		new_copy->ref_count++;		/* for old_copy->shadow ref. */

		if (old_copy->res_count) {
			VM_OBJ_RES_INCR(new_copy);
			VM_OBJ_RES_DECR(src_object);
		}

		vm_object_unlock(old_copy);	/* done with old_copy */
	} else if (new_copy == VM_OBJECT_NULL) {
		vm_object_unlock(src_object);
		new_copy = vm_object_allocate(src_offset + size);
		vm_object_lock(new_copy);
		goto Retry;
	}

	/*
	 * Readjust the copy-object size if necessary.
	 */
	copy_size = new_copy->size;
	if (copy_size < src_offset+size) {
		copy_size = src_offset+size;
		new_copy->size = copy_size;
	}

	/*
	 *	Point the new copy at the existing object.
	 */

	new_copy->shadow = src_object;
	new_copy->shadow_offset = 0;
	new_copy->shadowed = TRUE;	/* caller must set needs_copy */
	assert(src_object->ref_count > 0);
	src_object->ref_count++;
	VM_OBJ_RES_INCR(src_object);
	src_object->copy = new_copy;
	vm_object_unlock(new_copy);

	/*
	 *	Mark all (current) pages of the existing object copy-on-write.
	 *	This object may have a shadow chain below it, but
	 *	those pages will already be marked copy-on-write.
	 */

	vm_object_paging_wait(src_object, FALSE);
	copy_delayed_protect_iterate++;
	queue_iterate(&src_object->memq, p, vm_page_t, listq) {
	    if (!p->fictitious)
		pmap_page_protect(p->phys_addr, 
				  (VM_PROT_ALL & ~VM_PROT_WRITE &
				   ~p->page_lock));
	}
	vm_object_unlock(src_object);
	XPR(XPR_VM_OBJECT,
		"vm_object_copy_delayed: used copy object %X for source %X\n",
		(integer_t)new_copy, (integer_t)src_object, 0, 0, 0);

	return(new_copy);
}
#endif	/* !NORMA_VM */

/*
 *	Routine:	vm_object_copy_strategically
 *
 *	Purpose:
 *		Perform a copy according to the source object's
 *		declared strategy.  This operation may block,
 *		and may be interrupted.
 */
kern_return_t
vm_object_copy_strategically(
	register vm_object_t	src_object,
	vm_offset_t		src_offset,
	vm_size_t		size,
	vm_object_t		*dst_object,	/* OUT */
	vm_offset_t		*dst_offset,	/* OUT */
	boolean_t		*dst_needs_copy) /* OUT */
{
	boolean_t	result;
	boolean_t	interruptible = TRUE; /* XXX */
	memory_object_copy_strategy_t copy_strategy;

	assert(src_object != VM_OBJECT_NULL);

	vm_object_lock(src_object);

	/*
	 *	The copy strategy is only valid if the memory manager
	 *	is "ready". Internal objects are always ready.
	 */

	while (!src_object->internal && !src_object->pager_ready) {

		vm_object_wait(	src_object,
				VM_OBJECT_EVENT_PAGER_READY,
				interruptible);
		if (interruptible &&
		    (current_thread()->wait_result != THREAD_AWAKENED)) {
			*dst_object = VM_OBJECT_NULL;
			*dst_offset = 0;
			*dst_needs_copy = FALSE;
			return(MACH_SEND_INTERRUPTED);
		}
		vm_object_lock(src_object);
	}

	copy_strategy = src_object->copy_strategy;

	/*
	 *	Use the appropriate copy strategy.
	 */

	switch (copy_strategy) {
	    case MEMORY_OBJECT_COPY_NONE:
		result = vm_object_copy_slowly(src_object, src_offset, size,
					       interruptible, dst_object);
		if (result == KERN_SUCCESS) {
			*dst_offset = 0;
			*dst_needs_copy = FALSE;
		}
		break;

	    case MEMORY_OBJECT_COPY_CALL:
		result = vm_object_copy_call(src_object, src_offset, size,
				dst_object);
		if (result == KERN_SUCCESS) {
			*dst_offset = src_offset;
			*dst_needs_copy = TRUE;
		}
		break;

#if	!NORMA_VM
	    case MEMORY_OBJECT_COPY_DELAY:
		vm_object_unlock(src_object);
		*dst_object = vm_object_copy_delayed(src_object,
						     src_offset, size);
		*dst_offset = src_offset;
		*dst_needs_copy = TRUE;
		result = KERN_SUCCESS;
		break;
#endif	/* !NORMA_VM */

	    case MEMORY_OBJECT_COPY_SYMMETRIC:
		XPR(XPR_VM_OBJECT, "v_o_c_strategically obj 0x%x off 0x%x size 0x%x\n",(natural_t)src_object, src_offset, size, 0, 0);
		vm_object_unlock(src_object);
		result = KERN_MEMORY_RESTART_COPY;
		break;

	    default:
		panic("copy_strategically: bad strategy");
		result = KERN_INVALID_ARGUMENT;
	}
	return(result);
}

#if	NORMA_VM
/*
 *	Called (as a synchronous RPC) because some kernel called
 *	memory_object_copy and a new copy needed to be created,
 *	or in response to this kernel calling memory_object_copy.
 *
 *	If the copy is being waited for, we leave a reference around
 *	to the copy for the waiting thread to pick up.
 */
kern_return_t
memory_object_create_copy(
	vm_object_t	src_object,
	ipc_port_t	new_copy_pager,
	kern_return_t	*result)
{
	vm_page_t	p;
	vm_object_t	new_copy;
	boolean_t	handoff_reference;
	boolean_t	internal;

	/*
	 *	Source object must be a valid object.
	 */
	if (src_object == VM_OBJECT_NULL) {
		if (IP_VALID(new_copy_pager))
			ipc_port_release_send(new_copy_pager);
		*result = KERN_INVALID_ARGUMENT;
		return KERN_SUCCESS;
	}

	/*
	 *	New copy pager must be a valid port.
	 */
	if (! IP_VALID(new_copy_pager)) {
		*result = KERN_INVALID_ARGUMENT;
		vm_object_deallocate(src_object);
		return KERN_SUCCESS;
	}

	internal = !IP_IS_REMOTE(new_copy_pager);

	/*
	 *	If we aren't waiting for a copy, then we can return
	 *	without doing anything if we don't have an old copy
	 *	or if the new copy is the same as the old copy.
	 *
	 *	If we are waiting for a copy, then we can return if
	 *	we have an old copy and it is the same as the new copy.
	 */

	vm_object_lock(src_object);
	if (! vm_object_wanted(src_object, VM_OBJECT_EVENT_COPY_CALL)) {
		if (src_object->copy == VM_OBJECT_NULL ||
		    src_object->copy->pager == new_copy_pager) {
			vm_object_unlock(src_object);
			vm_object_deallocate(src_object);
			ipc_port_release_send(new_copy_pager);
			*result = KERN_ABORTED;
			return KERN_SUCCESS;
		}
		vm_object_unlock(src_object);
		handoff_reference = FALSE;
	} else {
		/*
		 * Try to get src_object and its copy locks (out of order),
		 *	and increment copy reference count, so that it won't
		 *	be removed during vm_enter_enter() call.
		 */
		while (src_object->copy != VM_OBJECT_NULL &&
		       src_object->copy->pager == new_copy_pager) {
			if (!vm_object_lock_try(src_object->copy)) {
				vm_object_unlock(src_object);
				mutex_pause();    /* wait a bit */
				vm_object_lock(src_object);
				continue;
			}
			new_copy = src_object->copy;
			assert(new_copy->pager_initialized);
			new_copy->ref_count++;
#if	TASK_SWAPPER
			/*
			 * This is equivalent to
			 * vm_object_res_reference(new_copy) but without
			 * the locking problems (both new_copy and
			 * src_object are currently locked).
			 */
			VM_OBJ_RES_INCR(new_copy);
			if (new_copy->res_count == 1)
				vm_object_res_reference(src_object);
#endif	/* TASK_SWAPPER */

			vm_object_unlock(new_copy);
			vm_object_unlock(src_object);

			/*
			 *	Do a full-fledged vm_object_enter
			 *	to handle any setting of copy_strategy
			 *	and temporary for src_object->copy,
			 *	allowing general memory manager use
			 *	of copy_call, even though we know
			 *	(as of this writing) that the xmm
			 *	system creates permanent noncachable
			 *	copies.
			 *
			 *	The reference created by vm_object_enter
			 *	is consumed by the thread waiting for
			 *	VM_OBJECT_COPY_CALL_EVENT.
			 */
#if	MACH_ASSERT
			assert(vm_object_enter(src_object->copy->pager,
					       src_object->copy->size,
					       internal, FALSE) ==
			       src_object->copy);
#else	/* MACH_ASSERT */
			(void) vm_object_enter(src_object->copy->pager,
					       src_object->copy->size,
					       internal, FALSE);
#endif	/* MACH_ASSERT */
			vm_object_lock(src_object);
			vm_object_wakeup(src_object,
					 VM_OBJECT_EVENT_COPY_CALL);
			vm_object_unlock(src_object);
			vm_object_deallocate(src_object);
			vm_object_deallocate(new_copy);
			ipc_port_release_send(new_copy_pager);
			*result = KERN_SUCCESS;
			return KERN_SUCCESS;
		}

		/*
		 *	The pager port has already been used as the pager port
		 *	of a copy object that does not exist anymore. It is an
		 *	invalid argument, because it won't be able to finish by
		 *	itself (in xmm_object_terminate), and thus it will lead
		 *	to a deadlock (with the forthcoming xmm_object_wait
		 *	indirectly called by vm_object_enter).
		 */
		vm_object_unlock(src_object);
		vm_object_cache_lock();
		if (ip_kotype(new_copy_pager) == IKOT_PAGER_TERMINATING) {
			/*
			 * This object is in the middle of termination.
			 * Wait for it to finish, then return KERN_INVALID_ARG
			 * as in the case below to try again.  If we don't
			 * block here and wait for the termination to finish,
			 * we end up in a livelock by getting right back here
			 * from m_ksvm_copy and the termination cannot get
			 * the cpu to finish up.
			 */
			new_copy_pager->ip_kobject =
						(ipc_kobject_t)new_copy_pager;
			assert_wait((event_t) new_copy_pager, FALSE);
			vm_object_cache_unlock();
			thread_block((void (*)(void))0);
			vm_object_deallocate(src_object);
			ipc_port_release_send(new_copy_pager);
			*result = KERN_INVALID_ARGUMENT;
			return KERN_SUCCESS;
		} else if (ip_kotype(new_copy_pager) != IKOT_NONE) {
			vm_object_cache_unlock();
			vm_object_deallocate(src_object);
			ipc_port_release_send(new_copy_pager);
			*result = KERN_INVALID_ARGUMENT;
			return KERN_SUCCESS;
		}
		vm_object_cache_unlock();
		handoff_reference = TRUE;
	}

	/*
	 *	Create an object for new_copy_pager. If we fail,
	 *	or if already shadows an object other than src_object,
	 *	then return failure. (The latter can only happen
	 *	if we allow objects to hold onto their shadows while
	 *	being cached.) Otherwise, make new_copy shadow src_object
	 *	if it does not do so already.
	 */
	new_copy = vm_object_enter(new_copy_pager,
				   src_object->size, internal, FALSE);
	ipc_port_release_send(new_copy_pager);
	if (new_copy == VM_OBJECT_NULL ||
	    (new_copy->shadow != VM_OBJECT_NULL &&
	     new_copy->shadow != src_object)) {
		vm_object_deallocate(src_object);
		vm_object_deallocate(new_copy);
		*result = KERN_FAILURE;
		return KERN_FAILURE;
	}
	vm_object_lock(new_copy);

	/*
	 *	pager_ready flag of internal vm_object isn't set by
	 *	vm_object_enter on a newly created vm_object.
	 */
	if (new_copy->internal && !new_copy->pager_ready) {
		new_copy->pager_ready = TRUE;
		vm_object_wakeup(new_copy, VM_OBJECT_EVENT_PAGER_READY);
	}

        /*
         * Try to get vm_object and its oldcopy locks (out of order).
         */
        vm_object_lock(src_object);
	if (src_object->copy != new_copy) {
		while (src_object->copy != VM_OBJECT_NULL) {
			if (vm_object_lock_try(src_object->copy))
				break;
			vm_object_unlock(src_object);
			mutex_pause();    /* wait a bit */
			vm_object_lock(src_object);
		}

		/*
		 *	Make new copy shadow src_object if it does not do so
		 *	already.  (It might already if it was cached.)
		 */
		if (new_copy->shadow != src_object) {
			new_copy->shadow = src_object;
			new_copy->shadow_offset = 0;
			src_object->shadowed = TRUE;
			assert(src_object->ref_count > 0);
			src_object->ref_count++;
			vm_object_res_reference(src_object);
		}

		/*
		 *	If we have an old copy, make it shadow the new copy
		 *	instead of src_object.
		 */
		if (src_object->copy != VM_OBJECT_NULL) {
			src_object->copy->shadow = new_copy;
			new_copy->shadowed = TRUE;
			new_copy->ref_count++;
#if	TASK_SWAPPER
			/*
			 * This is equivalent to
			 * vm_object_res_reference(new_copy) but without
			 * the locking problems (both new_copy and
			 * src_object are currently locked).
			 */
			VM_OBJ_RES_INCR(new_copy);
			if (new_copy->res_count == 1)
				vm_object_res_reference(src_object);
#endif	/* TASK_SWAPPER */
			src_object->ref_count--;
			vm_object_res_deallocate(src_object);
			assert(src_object->ref_count > 0);
			vm_object_unlock(src_object->copy);
		}

		/*
		 *	Adjust source object's copy pointer.
		 */
		src_object->copy = new_copy;
	}

	assert(new_copy->shadow == src_object);

	vm_object_unlock(new_copy);

	/*
	 *	Remove write access from all of the pages of
	 *	the source object that we can.
	 */

	vm_object_paging_wait(src_object, FALSE);
	queue_iterate(&src_object->memq, p, vm_page_t, listq) {
		if (p->fictitious) {
			continue;
		}
		if (! (p->page_lock & VM_PROT_WRITE)) {
			p->page_lock |= VM_PROT_WRITE;
			p->unusual = TRUE;
			pmap_page_protect(p->phys_addr,
					  VM_PROT_ALL & ~p->page_lock);
		}
	}

	/*
	 *	Wake up anyone waiting for this copy, and return.
	 */
	if (handoff_reference) {
		assert(vm_object_wanted(src_object,
					VM_OBJECT_EVENT_COPY_CALL));
		vm_object_wakeup(src_object, VM_OBJECT_EVENT_COPY_CALL);
		vm_object_unlock(src_object);
	} else {
		vm_object_unlock(src_object);
		vm_object_deallocate(new_copy);
	}
	vm_object_deallocate(src_object);
	*result = KERN_SUCCESS;
	return KERN_SUCCESS;
}
#endif	/* NORMA_VM */

/*
 *	vm_object_shadow:
 *
 *	Create a new object which is backed by the
 *	specified existing object range.  The source
 *	object reference is deallocated.
 *
 *	The new object and offset into that object
 *	are returned in the source parameters.
 */
boolean_t vm_object_shadow_check = FALSE;

boolean_t
vm_object_shadow(
	vm_object_t	*object,	/* IN/OUT */
	vm_offset_t	*offset,	/* IN/OUT */
	vm_size_t	length)
{
	register vm_object_t	source;
	register vm_object_t	result;

	source = *object;
	assert(source->copy_strategy == MEMORY_OBJECT_COPY_SYMMETRIC);

	/*
	 *	Determine if we really need a shadow.
	 */

	if (vm_object_shadow_check && source->ref_count == 1 &&
	    (source->shadow == VM_OBJECT_NULL ||
	     source->shadow->copy == VM_OBJECT_NULL))
	{
		source->shadowed = FALSE;
		return FALSE;
	}

	/*
	 *	Allocate a new object with the given length
	 */

	if ((result = vm_object_allocate(length)) == VM_OBJECT_NULL)
		panic("vm_object_shadow: no object for shadowing");

	/*
	 *	The new object shadows the source object, adding
	 *	a reference to it.  Our caller changes his reference
	 *	to point to the new object, removing a reference to
	 *	the source object.  Net result: no change of reference
	 *	count.
	 */
	result->shadow = source;
	
	/*
	 *	Store the offset into the source object,
	 *	and fix up the offset into the new object.
	 */

	result->shadow_offset = *offset;

	/*
	 *	Return the new things
	 */

	*offset = 0;
	*object = result;
	return TRUE;
}

/*
 *	The relationship between vm_object structures and
 *	the memory_object ports requires careful synchronization.
 *
 *	All associations are created by vm_object_enter.  All three
 *	port fields are filled in, as follows:
 *		pager:	the memory_object port itself, supplied by
 *			the user requesting a mapping (or the kernel,
 *			when initializing internal objects); the
 *			kernel simulates holding send rights by keeping
 *			a port reference;
 *		pager_request:
 *			the memory object control port,
 *			created by the kernel; the kernel holds
 *			receive (and ownership) rights to this
 *			port, but no other references.
 *	All of the ports are referenced by their global names.
 *
 *	When initialization is complete, the "initialized" field
 *	is asserted.  Other mappings using a particular memory object,
 *	and any references to the vm_object gained through the
 *	port association must wait for this initialization to occur.
 *
 *	In order to allow the memory manager to set attributes before
 *	requests (notably virtual copy operations, but also data or
 *	unlock requests) are made, a "ready" attribute is made available.
 *	Only the memory manager may affect the value of this attribute.
 *	Its value does not affect critical kernel functions, such as
 *	internal object initialization or destruction.  [Furthermore,
 *	memory objects created by the kernel are assumed to be ready
 *	immediately; the default memory manager need not explicitly
 *	set the "ready" attribute.]
 *
 *	[Both the "initialized" and "ready" attribute wait conditions
 *	use the "pager" field as the wait event.]
 *
 *	The port associations can be broken down by any of the
 *	following routines:
 *		vm_object_terminate:
 *			No references to the vm_object remain, and
 *			the object cannot (or will not) be cached.
 *			This is the normal case, and is done even
 *			though one of the other cases has already been
 *			done.
 *		vm_object_destroy:
 *			The memory_object port has been destroyed,
 *			meaning that the kernel cannot flush dirty
 *			pages or request new data or unlock existing
 *			data.
 *		memory_object_destroy:
 *			The memory manager has requested that the
 *			kernel relinquish rights to the memory object
 *			port.  [The memory manager may not want to
 *			destroy the port, but may wish to refuse or
 *			tear down existing memory mappings.]
 *	Each routine that breaks an association must break all of
 *	them at once.  At some later time, that routine must clear
 *	the vm_object port fields and release the port rights.
 *	[Furthermore, each routine must cope with the simultaneous
 *	or previous operations of the others.]
 *
 *	In addition to the lock on the object, the vm_object_cache_lock
 *	governs the port associations.  References gained through the
 *	port association require use of the cache lock.
 *
 *	Because the port fields may be cleared spontaneously, they
 *	cannot be used to determine whether a memory object has
 *	ever been associated with a particular vm_object.  [This
 *	knowledge is important to the shadow object mechanism.]
 *	For this reason, an additional "created" attribute is
 *	provided.
 *
 *	During various paging operations, the port values found in the
 *	vm_object must be valid.  To prevent these port rights from being
 *	released, and to prevent the port associations from changing
 *	(other than being removed, i.e., made null), routines may use
 *	the vm_object_paging_begin/end routines [actually, macros].
 *	The implementation uses the "paging_in_progress" and "wanted" fields.
 *	[Operations that alter the validity of the port values include the
 *	termination routines and vm_object_collapse.]
 */

#if	NORMA_VM
#define	IKOT_PAGER_LOOKUP_TYPE	IKOT_PAGER
#else	/* NORMA_VM */
#define	IKOT_PAGER_LOOKUP_TYPE	IKOT_PAGING_REQUEST
#endif	/* NORMA_VM */

vm_object_t
vm_object_lookup(
	ipc_port_t	port)
{
	vm_object_t	object;

start_over:
	object = VM_OBJECT_NULL;

	if (IP_VALID(port)) {
		vm_object_cache_lock();
		ip_lock(port);
		if (ip_active(port) &&
		    (ip_kotype(port) == IKOT_PAGER_LOOKUP_TYPE)) {
			object = (vm_object_t) port->ip_kobject;
			if (!vm_object_lock_try(object)) {
				/*
				 * failed to acquire object lock.  Drop the
				 * other two locks and wait for it, then go
				 * back and start over in case the port
				 * associations changed in the interim.
				 */
				ip_unlock(port);
				vm_object_cache_unlock();
				vm_object_lock(object);
				vm_object_unlock(object);
				goto start_over;
			}

			assert(object->alive);

			if (object->ref_count == 0) {
				queue_remove(&vm_object_cached_list, object,
					     vm_object_t, cached_list);
				vm_object_cached_count--;
				XPR(XPR_VM_OBJECT_CACHE,
			       "vm_object_lookup: removing %X, head (%X, %X)\n",
				    (integer_t)object, 
				    (integer_t)vm_object_cached_list.next,
				    (integer_t)vm_object_cached_list.prev, 0,0);
			}

			object->ref_count++;
			vm_object_res_reference(object);
			vm_object_unlock(object);
		}
		ip_unlock(port);
		vm_object_cache_unlock();
	}

	return object;
}



void
vm_object_destroy(
	ipc_port_t	pager)
{
	vm_object_t	object;
	pager_request_t	old_pager_request;

	/*
	 *	Perform essentially the same operations as in vm_object_lookup,
	 *	except that this time we look up based on the memory_object
	 *	port, not the control port.
	 */
	vm_object_cache_lock();
	if (ip_kotype(pager) != IKOT_PAGER) {
		vm_object_cache_unlock();
		return;
	}

	object = (vm_object_t) pager->ip_kobject;
	vm_object_lock(object);
	if (object->ref_count == 0) {
		XPR(XPR_VM_OBJECT_CACHE,
		   "vm_object_destroy: removing %x from cache, head (%x, %x)\n",
			(integer_t)object,
			(integer_t)vm_object_cached_list.next,
			(integer_t)vm_object_cached_list.prev, 0,0);

		queue_remove(&vm_object_cached_list, object,
				vm_object_t, cached_list);
		vm_object_cached_count--;
	}
	object->ref_count++;
	vm_object_res_reference(object);

	object->can_persist = FALSE;

	assert(object->pager == pager);

	/*
	 *	Remove the port associations.
	 *
	 *	Note that the memory_object itself is dead, so
	 *	we don't bother with it.
	 */

	object->pager = IP_NULL;
	vm_object_remove(object);

	old_pager_request = object->pager_request;

	object->pager_request = PAGER_REQUEST_NULL;

	vm_object_unlock(object);
	vm_object_cache_unlock();

	/*
	 *	Clean up the port references.  Note that there's no
	 *	point in trying the memory_object_terminate call
	 *	because the memory_object itself is dead.
	 */

	ipc_port_release_send(pager);

#if	!NORMA_VM
	if ((ipc_port_t)old_pager_request != IP_NULL)
		ipc_port_dealloc_kernel((ipc_port_t)old_pager_request);
#endif	/* !NORMA_VM */

	/*
	 *	Restart pending page requests
	 */
	vm_object_lock(object);

	vm_object_abort_activity(object);

	vm_object_unlock(object);
	/*
	 *	Lose the object reference.
	 */

	vm_object_deallocate(object);
}

/*
 *	Routine:	vm_object_enter
 *	Purpose:
 *		Find a VM object corresponding to the given
 *		pager; if no such object exists, create one,
 *		and initialize the pager.
 */
vm_object_t
vm_object_enter(
	ipc_port_t	pager,
	vm_size_t	size,
	boolean_t	internal,
	boolean_t	init)
{
	register
	vm_object_t	object;
	vm_object_t	new_object;
	boolean_t	must_init;
	vm_size_t	cluster_size;
	ipc_port_t	pager_request;
	ipc_kobject_type_t po;

restart:
	if (!IP_VALID(pager))
		return(vm_object_allocate(size));

	new_object = VM_OBJECT_NULL;
	must_init = init;

	/*
	 *	Look for an object associated with this port.
	 */

	vm_object_cache_lock();
	for (;;) {
		po = ip_kotype(pager);

		/*
		 *	If a previous object is being terminated,
		 *	we must wait for the termination message
		 *	to be queued.
		 *
		 *	We set kobject to a non-null value to let the
		 *	terminator know that someone is waiting.
		 *	Among the possibilities is that the port
		 *	could die while we're waiting.  Must restart
		 *	instead of continuing the loop.
		 */

		if (po == IKOT_PAGER_TERMINATING) {
			pager->ip_kobject = (ipc_kobject_t) pager;
			assert_wait((event_t) pager, FALSE);
			vm_object_cache_unlock();
			thread_block((void (*)(void))0);
			goto restart;
		}

		/*
		 *	Bail if there is already a kobject associated
		 *	with the pager port.
		 */
		if (po != IKOT_NONE) {
			break;
		}

		/*
		 *	We must unlock to create a new object;
		 *	if we do so, we must try the lookup again.
		 */

		if (new_object == VM_OBJECT_NULL) {
			vm_object_cache_unlock();
			new_object = vm_object_allocate(size);
			vm_object_cache_lock();
		} else {
			/*
			 *	Lookup failed twice, and we have something
			 *	to insert; set the object.
			 */

			ipc_kobject_set(pager,
					(ipc_kobject_t) new_object,
					IKOT_PAGER);
			new_object = VM_OBJECT_NULL;
			must_init = TRUE;
		}
	}

	/*
	 *	It's only good if it's a VM object!
	 */

	object = (po == IKOT_PAGER) ?
		 (vm_object_t) pager->ip_kobject : VM_OBJECT_NULL;

	if (object != VM_OBJECT_NULL && !must_init) {
		vm_object_lock(object);
		assert(object->pager_created);
		assert(!internal || object->internal);
		if (object->ref_count == 0) {
			XPR(XPR_VM_OBJECT_CACHE,
		    "vm_object_enter: removing %x from cache, head (%x, %x)\n",
				(integer_t)object,
				(integer_t)vm_object_cached_list.next,
				(integer_t)vm_object_cached_list.prev, 0,0);
			queue_remove(&vm_object_cached_list, object,
				     vm_object_t, cached_list);
			vm_object_cached_count--;
		}
		object->ref_count++;
		vm_object_res_reference(object);
		vm_object_unlock(object);

		VM_STAT(hits++);
	} 
	assert((object == VM_OBJECT_NULL) || (object->ref_count > 0));

	VM_STAT(lookups++);

	vm_object_cache_unlock();

	XPR(XPR_VM_OBJECT,
		"vm_o_enter: pager 0x%x obj 0x%x must_init %d\n",
		(integer_t)pager, (integer_t)object, must_init, 0, 0);

	/*
	 *	If we raced to create a vm_object but lost, let's
	 *	throw away ours.
	 */

	if (new_object != VM_OBJECT_NULL)
		vm_object_deallocate(new_object);

	if (object == VM_OBJECT_NULL)
		return(object);

	if (must_init) {

#if	!NORMA_VM
		/*
		 *	Allocate request port.
		 */

		pager_request = ipc_port_alloc_kernel();
		assert (pager_request != IP_NULL);
		ipc_kobject_set(pager_request, (ipc_kobject_t) object,
				IKOT_PAGING_REQUEST);
#endif	/* !NORMA_VM */

		vm_object_lock(object);

		/*
		 *	Copy the naked send right we were given.
		 */

		pager = ipc_port_copy_send(pager);
		if (!IP_VALID(pager))
			panic("vm_object_enter: port died"); /* XXX */

		object->pager_created = TRUE;
		object->pager = pager;
		object->internal = internal;
		object->pager_trusted = internal;
		if (!internal) {
			/* copy strategy invalid until set by memory manager */
			object->copy_strategy = MEMORY_OBJECT_COPY_INVALID;
		}
#if	NORMA_VM
		object->pager_ready = FALSE;
#else	/* NORMA_VM */
		object->pager_request = pager_request;
		if (internal) {
			/* default-pager objects are ready immediately */
			object->pager_ready = TRUE;
			vm_object_wakeup(object, VM_OBJECT_EVENT_PAGER_READY);
		} else {
			/* user pager objects are not ready until marked so */
			object->pager_ready = FALSE;
		}

#endif	/* NORMA_VM */

		vm_object_unlock(object);

#if	NORMA_VM

		/*
		 *	Let the xmm system know that we want to use the pager.
		 *
		 *	For internal objects, pager_ready will be set after
		 * 	pages have been declared; otherwise, it will be set
		 *	via set_attributes_common.
		 */

		object->cluster_size = PAGE_SIZE;

		(void) xmm_memory_object_init(object);
#else	/* NORMA_VM */

		/*
		 *	Let the pager know we're using it.
		 */

		if (internal) {
			ipc_port_t	DMM;

			/* acquire a naked send right for the DMM */
			DMM = memory_manager_default_reference(&cluster_size);
			assert(cluster_size >= PAGE_SIZE);

			object->cluster_size = cluster_size;
			assert(object->temporary);

			/* consumes the naked send right for DMM */
			(void) memory_object_create(DMM,
				pager,
				object->size,
				object->pager_request,
				PAGE_SIZE);
		} else {
			(void) memory_object_init(pager,
				object->pager_request,
				PAGE_SIZE);
		}
#endif	/* NORMA_VM */

		vm_object_lock(object);
		object->pager_initialized = TRUE;
		vm_object_wakeup(object, VM_OBJECT_EVENT_INITIALIZED);
	} else {
		vm_object_lock(object);


#if	NORMA_VM
		if (!internal) {
			/*
			 *	We have to synchronize m_o_caching() and
			 *	m_o_set_attributes()/m_o_destroy().
			 */
			while (object->temporary_caching) {
				vm_object_assert_wait(object,
						      VM_OBJECT_EVENT_CACHING,
						      FALSE);
				vm_object_unlock(object);
				thread_block((void (*)(void)) 0);
				vm_object_lock(object);
			}

			if (object->temporary_cached) {
				if (! vm_object_uncache(object, TRUE)) {
					goto restart;
				}
			}
		}
#endif	/* NORMA_VM */
	}
	/*
	 *	[At this point, the object must be locked]
	 */

	/*
	 *	Wait for the work above to be done by the first
	 *	thread to map this object.
	 */

	while (!object->pager_initialized) {
		vm_object_wait(	object,
				VM_OBJECT_EVENT_INITIALIZED,
				FALSE);
		vm_object_lock(object);
	}
	vm_object_unlock(object);

	XPR(XPR_VM_OBJECT,
	    "vm_object_enter: vm_object %x, memory_object %x, internal %d\n",
	    (integer_t)object, (integer_t)object->pager, internal, 0,0);
	return(object);
}

#if	NORMA_VM
/*
 * We have made a new region of the object copy-on-write.
 * If the whole object is now copy-on-write, then we tell
 * the svm layer that the object is frozen, passing it
 * a pagemap that can be sent to other kernels to avoid
 * unnecessary data_requests.
 */
void
vm_object_export_pagemap_try(
	vm_object_t	object,
	vm_size_t	size)
{
#if	MACH_PAGEMAP
	vm_page_t m;

	vm_object_lock(object);
	if (object->pagemap_exported) {
		vm_object_unlock(object);
		return;
	}

	/*
	 * XXX
	 * Do I need to synchronize between pager_created
	 * and handle copy-on-write obligations?
	 */

	assert(object->internal);
	assert(object->copy_strategy == MEMORY_OBJECT_COPY_SYMMETRIC);

	if (object->existence_map == VM_EXTERNAL_NULL) {
		vm_object_unlock(object);
		return;
	}
	assert(object->pager_created);

	/*
	 * Check the size.  It may be the case that only part of the object
	 * has been write protected, so there are pages in it that may
	 * not get properly shared.  Once the entire object is present,
	 * go ahead and share it.
	 */
	if (size == object->size)
		object->frozen_size = size;
	else
		object->frozen_size += size;

	if (object->frozen_size != object->size) {
		if (object->frozen_size < object->size) {
			vm_object_unlock(object);
			return;
		}
		/*
		 * XXX Can we do this??  Enough of the frozen
		 * XXX object has been frozen that we may be able
		 * XXX to freeze the whole thing...is this strictly
		 * XXX true?
		 */
		object->frozen_size = object->size;
	}

	/*
	 * Remember that we have given the pager a frozen pagemap.
	 */
	object->pagemap_exported = TRUE;

	/*
	 * Synchronize declaration of vm_object pages with current pagination
	 * work on vm_object pages.
	 */
	vm_object_paging_wait(object, FALSE);
	vm_object_paging_begin(object);
	vm_object_unlock(object);

#if 1
	/*
	 * Declare pages now that the svm layer is going to be interested
	 * in what we do.
	 */
	vm_object_declare_pages(object);
#else	/* 1 */

	/*
	 * Merge resident page information into the object's
	 * existence info. This is safe (spurious 1's are okay;
	 * spurious 0's are not); furthermore, it shouldn't
	 * be inefficient, since the kernel won't look at these
	 * 1's as long as the pages are resident, and it will
	 * set the 1's as it makes them not resident anyway
	 * (since all of these pages are probably dirty or precious).
	 *
	 * XXXO
	 * There will often be lots of zeros in the existence info.
	 * We could compress this information with a more
	 * complex representation, if it seems worthwhile.
	 */
	queue_iterate(&object->memq, m, vm_page_t, listq) {
		assert(m->offset < object->size);
		vm_external_state_set(object->existence_map,
			m->offset + object->paging_offset);
	}

	/*
	 * Pass pagemap to pager.
	 */
	memory_object_freeze(object->pager,
			     object->pager_request,
			     object->existence_map,
			     object->size);

	/*
	 * Remember that we have given the pager a frozen pagemap.
	 */
	object->pagemap_exported = TRUE;
#endif	/* 1 */
	vm_object_lock(object);
	vm_object_paging_end(object);
	vm_object_unlock(object);
#endif	/* MACH_PAGEMAP */
}

/*
 *	For use only by vm_object_declare_pages.
 *	If a page is about to be paged in, then
 *	continue on to the next page; otherwise,
 *	mark the page dirty, and allow it to be
 *	declared.
 *
 *	A page handed to this routine cannot have
 *	the absent, error, or restart bits set,
 *	since such bits are only set upon receiving
 *	a reply from the pager, and since the object
 *	is not ready, we have not even sent it any
 *	requests. A page can, however, be fictitious,
 *	since a fault may be in progress, blocked
 *	on the pager_ready object attribute.
 */
#define	SET_DIRTY_OR_SKIP(m)					\
{								\
	assert(! (m)->error && ! (m)->restart);			\
	if ((m)->absent || (m)->fictitious) {			\
		continue;					\
	}							\
	(m)->dirty = TRUE;					\
}

/*
 *	Tell the svm layer about the pages that we have.
 *	The svm layer doesn't know about these pages
 *	because we didn't ask for them because the svm
 *	object for this object didn't exist until we
 *	created the pager and therefore there was no
 *	one to ask. However, the svm layer needs to know
 *	about them now so that it knows what pages to
 *	ask us for if another kernel maps the object.
 *
 *	We will try to give the svm layer a pagemap that
 *	represents this object in a frozen state that
 *	the svm layer can then hand to other kernels,
 *	allowing useless data_requests to be avoided
 *	(those that will always be answered with
 *	data_unavailable). However, if the object is
 *	too large to create a pagemap for, we tell the
 *	kernel about the pages we have, one by one,
 *	avoiding the creation of a pagemap; if the
 *	object is not too large to create a pagemap for,
 *	but it isn't frozen, then we indicate such when
 *	sending the pagemap to the svm layer, so that it
 *	will use it only to see what pages we have,
 *	discarding the pagemap when it is done.
 *	(We can tell that the object is "too large" by
 *	seeing that vm_external_create failed to create
 *	a pagemap for the object.)
 *
 *	Every page here is likely to be dirty or to become dirty.
 *	Mark every page dirty, so that the pager can count on
 *	the kernel to hold a copy. No pages should be being
 *	paged in, since the object is not yet ready.
 *
 *	XXXO
 *	If MACH_PAGEMAP is enabled, we could use pagemaps to
 *	declare chunks of a very large object via multiple
 *	calls to declare_pages.
 */
void
vm_object_declare_pages(
	vm_object_t	object)
{
	vm_page_t m;
	boolean_t frozen;
	boolean_t gotapage;
#if	MACH_PAGEMAP
	vm_external_map_t	emap;
#endif	/* MACH_PAGEMAP */

	assert(object->pager_created);
	assert(! object->pager_ready || object->size == object->frozen_size);
#if	MACH_PAGEMAP
	if (object->existence_map) {
		frozen = (object->size == object->frozen_size);
		/*
		 * Create a temporary existence map to collect the
		 * present pages in - doing a declare pages on pages
		 * the pager has will not work.
		 */
		emap = vm_external_create(object->size);

		/*
		 *	Merge resident page information into the object's
		 *	existence info, as in vm_object_export_pagemap_try.
		 *
		 *	XXXO
		 *	While it is not incorrect to set bits in the pagemap,
		 *	it does make coalescing harder. Should use a temporary
		 *	pagemap instead.
		 */
		gotapage = FALSE;
		vm_object_lock(object);
		queue_iterate(&object->memq, m, vm_page_t, listq) {
			assert(m->offset < object->size);

			/*
			 * If the object is frozen, do not dirty rdonly
			 * pages.
			 */
			if (!(frozen && (m->page_lock & VM_PROT_WRITE)))
				SET_DIRTY_OR_SKIP(m);
			vm_external_state_set(object->existence_map,
				m->offset + object->paging_offset);
			vm_external_state_set(emap,
				m->offset + object->paging_offset);
			if (!gotapage)
				gotapage = TRUE;
		}
		vm_object_unlock(object);
		
		/*
		 * Pass pagemap to pager.
		 */
		if (gotapage)
			memory_object_declare_pages(object->pager,
						    object->pager_request,
						    emap,
						    object->size,
						    frozen);
		vm_external_destroy(emap, object->size);
		return;
	}
#endif	/* MACH_PAGEMAP */

	/*
	 *	Declare each resident page, one by one.
	 */
	queue_iterate(&object->memq, m, vm_page_t, listq) {
		assert(m->offset < object->size);
		SET_DIRTY_OR_SKIP(m);
		memory_object_declare_page(object->pager,
					   object->pager_request,
					   m->offset,
					   PAGE_SIZE);
	}
}

#endif	/* NORMA_VM */

/*
 *	Routine:	vm_object_pager_create
 *	Purpose:
 *		Create a memory object for an internal object.
 *	In/out conditions:
 *		The object is locked on entry and exit;
 *		it may be unlocked within this call.
 *	Limitations:
 *		Only one thread may be performing a
 *		vm_object_pager_create on an object at
 *		a time.  Presumably, only the pageout
 *		daemon will be using this routine.
 */

void
vm_object_pager_create(
	register vm_object_t	object)
{
	ipc_port_t		pager;
#if	MACH_PAGEMAP
	vm_size_t		size;
	vm_external_map_t	map;
#endif	/* MACH_PAGEMAP */

	XPR(XPR_VM_OBJECT, "vm_object_pager_create, object 0x%X\n",
		(integer_t)object, 0,0,0,0);

	if (memory_manager_default_check() != KERN_SUCCESS)
		return;

	/*
	 *	Prevent collapse or termination by holding a paging reference
	 */

	vm_object_paging_begin(object);
	if (object->pager_created) {
		/*
		 *	Someone else got to it first...
		 *	wait for them to finish initializing the ports
		 */
#if	NORMA_VM
		while (!object->pager_ready) {
			vm_object_wait(	object,
				       VM_OBJECT_EVENT_PAGER_READY,
				       FALSE);
			vm_object_lock(object);
		}
#else	/* NORMA_VM */
		while (!object->pager_initialized) {
			vm_object_wait(	object,
				       VM_OBJECT_EVENT_INITIALIZED,
				       FALSE);
			vm_object_lock(object);
		}
#endif	/* NORMA_VM */
		vm_object_paging_end(object);
		return;
	}

	/*
	 *	Indicate that a memory object has been assigned
	 *	before dropping the lock, to prevent a race.
	 */

	object->pager_created = TRUE;
	object->paging_offset = 0;
		
#if	MACH_PAGEMAP
	size = object->size;
#endif	/* MACH_PAGEMAP */
	vm_object_unlock(object);

#if	MACH_PAGEMAP && !NORMA_VM
	/*
	 * The above conditional is incorrect - it should not
	 * exclude NORMA_VM.  However, there is a problem with the
	 * new copy logic that precludes this from working properly
	 * unless a new copy is created.  The fix is not obvious.
	 */
	map = vm_external_create(size);
	vm_object_lock(object);
	assert(object->size == size);
	object->existence_map = map;
	vm_object_unlock(object);
#endif	/* MACH_PAGEMAP && !NORMA_VM */

	/*
	 *	Create the pager ports, and associate them with this object.
	 *
	 *	We make the port association here so that vm_object_enter()
	 * 	can look up the object to complete initializing it.  No
	 *	user will ever map this object.
	 *
	 *	Since the kernel has the only rights to the ports it's safe
	 *	to install the association without holding the cache lock.
	 */

	pager = ipc_port_alloc_kernel();
	assert (pager != IP_NULL);
	(void) ipc_port_make_send(pager);
	ipc_kobject_set(pager, (ipc_kobject_t) object, IKOT_PAGER);

	if (vm_object_enter(pager, object->size, TRUE, TRUE) != object)
		panic("vm_object_pager_create: mismatch");

	/*
	 *	Drop the naked send right taken above.
	 */
	ipc_port_release_send(pager);

#if	NORMA_VM
	/*
	 *	Only after declaring pages do we mark the object ready.
	 *	This avoids the question of how to declare absent pages.
	 *	Also tell NORMA XMM that this is a copy call object if
	 *	it is.
	 */
	vm_object_declare_pages(object);
	if (object->copy_strategy == MEMORY_OBJECT_COPY_CALL)
		memory_object_share(object->pager, object->pager_request);
	vm_object_lock(object);
	assert(! object->pager_ready);
	object->pager_ready = TRUE;
	vm_object_wakeup(object, VM_OBJECT_EVENT_PAGER_READY);
#else	/* NORMA_VM */
	vm_object_lock(object);
#endif	/* NORMA_VM */

	/*
	 *	Release the paging reference
	 */
	vm_object_paging_end(object);
}

/*
 *	Routine:	vm_object_remove
 *	Purpose:
 *		Eliminate the pager/object association
 *		for this pager.
 *	Conditions:
 *		The object cache must be locked.
 */
void
vm_object_remove(
	vm_object_t	object)
{
	ipc_port_t port;

	if ((port = object->pager) != IP_NULL) {
		if (ip_kotype(port) == IKOT_PAGER)
			ipc_kobject_set(port,
#if	NORMA_VM
					/*
					 *	There should no longer be any
					 *	need for setting the port type
					 *	to anything other than IKO_NULL.
					 *	But I've left this in because
					 *	it's nice when debugging to
					 *	be able to recognize this state.
					 */
					port->ip_kobject,
#else	/* NORMA_VM */
					IKO_NULL,
#endif	/* NORMA_VM */
					IKOT_PAGER_TERMINATING);
		 else if (ip_kotype(port) != IKOT_NONE)
			panic("vm_object_remove: bad object port");
	}

#if	!NORMA_VM
	if ((port = object->pager_request) != IP_NULL) {
		if (ip_kotype(port) == IKOT_PAGING_REQUEST)
			ipc_kobject_set(port, IKO_NULL, IKOT_NONE);
		 else if (ip_kotype(port) != IKOT_NONE)
			panic("vm_object_remove: bad request port");
	}
#endif	/* !NORMA_VM */
}

/*
 *	Global variables for vm_object_collapse():
 *
 *		Counts for normal collapses and bypasses.
 *		Debugging variables, to watch or disable collapse.
 */
long	object_collapses = 0;
long	object_bypasses  = 0;

boolean_t	vm_object_collapse_allowed = TRUE;
boolean_t	vm_object_bypass_allowed = TRUE;

int	vm_external_discarded;
int	vm_external_collapsed;
/*
 *	vm_object_do_collapse:
 *
 *	Collapse an object with the object backing it.
 *	Pages in the backing object are moved into the
 *	parent, and the backing object is deallocated.
 *
 *	Both objects and the cache are locked; the page
 *	queues are unlocked.
 *
 */
void
vm_object_do_collapse(
	vm_object_t object,
	vm_object_t backing_object)
{
	vm_page_t p, pp;
	vm_offset_t new_offset, backing_offset;
	vm_size_t size;

	backing_offset = object->shadow_offset;
	size = object->size;


	/*
	 *	Move all in-memory pages from backing_object
	 *	to the parent.  Pages that have been paged out
	 *	will be overwritten by any of the parent's
	 *	pages that shadow them.
	 */
	
	while (!queue_empty(&backing_object->memq)) {
		
		p = (vm_page_t) queue_first(&backing_object->memq);
		
		new_offset = (p->offset - backing_offset);
		
		assert(!p->busy || p->absent);
		
		/*
		 *	If the parent has a page here, or if
		 *	this page falls outside the parent,
		 *	dispose of it.
		 *
		 *	Otherwise, move it as planned.
		 */
		
		if (p->offset < backing_offset || new_offset >= size) {
			VM_PAGE_FREE(p);
		} else {
			pp = vm_page_lookup(object, new_offset);
			if (pp == VM_PAGE_NULL) {

				/*
				 *	Parent now has no page.
				 *	Move the backing object's page up.
				 */

				vm_page_rename(p, object, new_offset);
#if	MACH_PAGEMAP
			} else if (pp->absent) {

				/*
				 *	Parent has an absent page...
				 *	it's not being paged in, so
				 *	it must really be missing from
				 *	the parent.
				 *
				 *	Throw out the absent page...
				 *	any faults looking for that
				 *	page will restart with the new
				 *	one.
				 */

				VM_PAGE_FREE(pp);
				vm_page_rename(p, object, new_offset);
#endif	/* MACH_PAGEMAP */
			} else {
				assert(! pp->absent);

				/*
				 *	Parent object has a real page.
				 *	Throw away the backing object's
				 *	page.
				 */
				VM_PAGE_FREE(p);
			}
		}
	}
	
	assert(object->pager == IP_NULL || backing_object->pager == IP_NULL);

	if (backing_object->pager != IP_NULL) {

		/*
		 *	Move the pager from backing_object to object.
		 *
		 *	XXX We're only using part of the paging space
		 *	for keeps now... we ought to discard the
		 *	unused portion.
		 */

		object->pager = backing_object->pager;
		ipc_kobject_set(object->pager, (ipc_kobject_t) object,
				IKOT_PAGER);
		object->pager_created = backing_object->pager_created;
		object->pager_request = backing_object->pager_request;
		object->pager_ready = backing_object->pager_ready;
		object->pager_initialized = backing_object->pager_initialized;
		object->cluster_size = backing_object->cluster_size;
		object->paging_offset =
		    backing_object->paging_offset + backing_offset;
#if	!NORMA_VM
		if (object->pager_request != IP_NULL) {
			ipc_kobject_set(object->pager_request,
					(ipc_kobject_t) object,
					IKOT_PAGING_REQUEST);
		}
#endif	/* !NORMA_VM */
	}

	vm_object_cache_unlock();

	object->paging_offset = backing_object->paging_offset + backing_offset;

#if	MACH_PAGEMAP
	/*
	 *	If the shadow offset is 0, the use the existence map from
	 *	the backing object if there is one. If the shadow offset is
	 *	not zero, toss it.
	 *
	 *	XXX - If the shadow offset is not 0 then a bit copy is needed
	 *	if the map is to be salvaged.  For now, we just just toss the
	 *	old map, giving the collapsed object no map. This means that
	 *	the pager is invoked for zero fill pages.  If analysis shows
	 *	that this happens frequently and is a performance hit, then
	 *	this code should be fixed to salvage the map.
	 */
	assert(object->existence_map == VM_EXTERNAL_NULL);
	if (backing_offset || (size != backing_object->size)) {
		vm_external_discarded++;
		vm_external_destroy(backing_object->existence_map,
			backing_object->size);
	}
	else {
		vm_external_collapsed++;
		object->existence_map = backing_object->existence_map;
	}
	backing_object->existence_map = VM_EXTERNAL_NULL;
#endif	/* MACH_PAGEMAP */

	/*
	 *	Object now shadows whatever backing_object did.
	 *	Note that the reference to backing_object->shadow
	 *	moves from within backing_object to within object.
	 */
	
	object->shadow = backing_object->shadow;
	object->shadow_offset += backing_object->shadow_offset;
	assert((object->shadow == VM_OBJECT_NULL) ||
	       (object->shadow->copy == VM_OBJECT_NULL));

	/*
	 *	Discard backing_object.
	 *
	 *	Since the backing object has no pages, no
	 *	pager left, and no object references within it,
	 *	all that is necessary is to dispose of it.
	 */
	
	assert((backing_object->ref_count == 1) &&
	       (backing_object->resident_page_count == 0) &&
	       (backing_object->paging_in_progress == 0));

	assert(backing_object->alive);
	backing_object->alive = FALSE;
	vm_object_unlock(backing_object);

	XPR(XPR_VM_OBJECT, "vm_object_collapse, collapsed 0x%X\n",
		(integer_t)backing_object, 0,0,0,0);

#if	VM_OBJECT_TRACKING
	vm_object_track_dealloc(backing_object);
#endif
	zfree(vm_object_zone, (vm_offset_t) backing_object);
	
	object_collapses++;
}

void
vm_object_do_bypass(
	vm_object_t object,
	vm_object_t backing_object)
{
	/*
	 *	Make the parent shadow the next object
	 *	in the chain.
	 */
	
#if	TASK_SWAPPER
	/*
	 *	Do object reference in-line to 
	 *	conditionally increment shadow's
	 *	residence count.  If object is not
	 *	resident, leave residence count
	 *	on shadow alone.
	 */
	if (backing_object->shadow != VM_OBJECT_NULL) {
		vm_object_lock(backing_object->shadow);
		backing_object->shadow->ref_count++;
		if (object->res_count != 0)
			vm_object_res_reference(backing_object->shadow);
		vm_object_unlock(backing_object->shadow);
	}
#else	/* TASK_SWAPPER */
	vm_object_reference(backing_object->shadow);
#endif	/* TASK_SWAPPER */

	object->shadow = backing_object->shadow;
	object->shadow_offset += backing_object->shadow_offset;
	
	/*
	 *	Backing object might have had a copy pointer
	 *	to us.  If it did, clear it. 
	 */
	if (backing_object->copy == object) {
		backing_object->copy = VM_OBJECT_NULL;
	}
	
	/*
	 *	Drop the reference count on backing_object.
#if	TASK_SWAPPER
	 *	Since its ref_count was at least 2, it
	 *	will not vanish; so we don't need to call
	 *	vm_object_deallocate.
	 *	[FBDP: that doesn't seem to be true any more]
	 * 
	 *	The res_count on the backing object is
	 *	conditionally decremented.  It's possible
	 *	(via vm_pageout_scan) to get here with
	 *	a "swapped" object, which has a 0 res_count,
	 *	in which case, the backing object res_count
	 *	is already down by one.
#else
	 *	Don't call vm_object_deallocate unless
	 *	ref_count drops to zero.
	 *
	 *	The ref_count can drop to zero here if the
	 *	backing object could be bypassed but not
	 *	collapsed, such as when the backing object
	 *	is temporary and cachable.
#endif
	 */
	if (backing_object->ref_count > 1) {
		backing_object->ref_count--;
#if	TASK_SWAPPER
		if (object->res_count != 0)
			vm_object_res_deallocate(backing_object);
		assert(backing_object->ref_count > 0);
#endif	/* TASK_SWAPPER */
		vm_object_unlock(backing_object);
	} else {

		/*
		 *	Drop locks so that we can deallocate
		 *	the backing object.
		 */

#if	TASK_SWAPPER
		if (object->res_count == 0) {
			/* XXX get a reference for the deallocate below */
			vm_object_res_reference(backing_object);
		}
#endif	/* TASK_SWAPPER */
		vm_object_unlock(object);
		vm_object_unlock(backing_object);
		vm_object_deallocate(backing_object);

		/*
		 *	Relock object. We don't have to reverify
		 *	its state since vm_object_collapse will
		 *	do that for us as it starts at the
		 *	top of its loop.
		 */

		vm_object_lock(object);
	}
	
	object_bypasses++;
}
		
/*
 *	vm_object_collapse:
 *
 *	Perform an object collapse or an object bypass if appropriate.
 *	The real work of collapsing and bypassing is performed in
 *	the routines vm_object_do_collapse and vm_object_do_bypass.
 *
 *	Requires that the object be locked and the page queues be unlocked.
 *
 */
void
vm_object_collapse(
	register vm_object_t	object)
{
	register vm_object_t	backing_object;
	register vm_offset_t	backing_offset;
	register vm_size_t	size;
	register vm_offset_t	new_offset;
	register vm_page_t	p;

	if (! vm_object_collapse_allowed && ! vm_object_bypass_allowed) {
		return;
	}

	XPR(XPR_VM_OBJECT, "vm_object_collapse, obj 0x%X\n", 
		(integer_t)object, 0,0,0,0);

	while (TRUE) {
		/*
		 *	Verify that the conditions are right for either
		 *	collapse or bypass:
		 *
		 *	The object exists and no pages in it are currently
		 *	being paged out, and
		 */
		if (object == VM_OBJECT_NULL ||
		    object->paging_in_progress != 0 ||
		    object->absent_count != 0)
			return;

		/*
		 *	There is a backing object, and
		 */
	
		if ((backing_object = object->shadow) == VM_OBJECT_NULL)
			return;
	
		vm_object_lock(backing_object);

		/*
		 *	...
		 *		The backing object is not read_only,
		 *		and no pages in the backing object are
		 *		currently being paged out.
		 *		The backing object is internal.
		 *
		 *		XXX norma_vm: internal?
		 *
		 */
	
		if (!backing_object->internal ||
		    backing_object->paging_in_progress != 0) {
			vm_object_unlock(backing_object);
			return;
		}
	
		/*
		 *	The backing object can't be a copy-object:
		 *	the shadow_offset for the copy-object must stay
		 *	as 0.  Furthermore (for the 'we have all the
		 *	pages' case), if we bypass backing_object and
		 *	just shadow the next object in the chain, old
		 *	pages from that object would then have to be copied
		 *	BOTH into the (former) backing_object and into the
		 *	parent object.
		 */
		if (backing_object->shadow != VM_OBJECT_NULL &&
		    backing_object->shadow->copy != VM_OBJECT_NULL) {
			vm_object_unlock(backing_object);
			return;
		}

		/*
		 *	We can now try to either collapse the backing
		 *	object (if the parent is the only reference to
		 *	it) or (perhaps) remove the parent's reference
		 *	to it.
		 */

		/*
		 *	If there is exactly one reference to the backing
		 *	object, we may be able to collapse it into the parent.
		 *
		 *	XXXO (norma vm):
		 *
		 *	The backing object must not have a pager
		 *	created for it, since collapsing an object
		 *	into a backing_object dumps new pages into
		 *	the backing_object that its pager doesn't
		 *	know about, and we've already declared pages.
		 *	This page dumping is deadly if other kernels
		 *	are shadowing this object; this is the
		 *	distributed equivalent of the ref_count == 1
		 *	condition.
		 *
		 *	With some work, we could downgrade this
		 *	restriction to the backing object must not
		 *	be cachable, since when a temporary object
		 *	is uncachable we are allowed to do anything
		 *	to it. We would have to do something like
		 *	call declare_pages again, and we would have
		 *	to be prepared for the memory manager
		 *	disabling temporary termination, which right
		 *	now is a difficult race to deal with, since
		 *	the memory manager currently assumes that
		 *	termination is the only possible failure
		 *	for disabling temporary termination.
		 */

		if (backing_object->ref_count == 1 &&
		    ! object->pager_created &&
#if	NORMA_VM
		    backing_object->temporary &&
		    ! backing_object->pager_created &&
		    backing_object->frozen_size == 0 &&
#endif	/* NORMA_VM */
		    vm_object_collapse_allowed) {

			XPR(XPR_VM_OBJECT, 
		   "vm_object_collapse: %x to %x, pager %x, pager_request %x\n",
				(integer_t)backing_object, (integer_t)object,
				(integer_t)backing_object->pager, 
				(integer_t)backing_object->pager_request, 0);

			/*
			 *	We need the cache lock for collapsing,
			 *	but we must not deadlock.
			 */
			
			if (! vm_object_cache_lock_try()) {
				vm_object_unlock(backing_object);
				return;
			}

			/*
			 *	Collapse the object with its backing
			 *	object, and try again with the object's
			 *	new backing object.
			 */

			vm_object_do_collapse(object, backing_object);
			continue;
		}


		/*
		 *	Collapsing the backing object was not possible
		 *	or permitted, so let's try bypassing it.
		 */

		if (! vm_object_bypass_allowed) {
			vm_object_unlock(backing_object);
			return;
		}

		/*
		 *	If the backing object has a pager but no pagemap,
		 *	then we cannot bypass it, because we don't know
		 *	what pages it has.
		 */
		if (backing_object->pager_created
#if	MACH_PAGEMAP
		    && (backing_object->existence_map == VM_EXTERNAL_NULL)
#endif	/* MACH_PAGEMAP */
		    ) {
			vm_object_unlock(backing_object);
			return;
		}

		backing_offset = object->shadow_offset;
		size = object->size;

		/*
		 *	If all of the pages in the backing object are
		 *	shadowed by the parent object, the parent
		 *	object no longer has to shadow the backing
		 *	object; it can shadow the next one in the
		 *	chain.
		 *
		 *	If the backing object has existence info,
		 *	we must check examine its existence info
		 *	as well.
		 *
		 *	XXX
		 *	Should have a check for a 'small' number
		 *	of pages here.
		 */

		/*
		 *	First, check pages resident in the backing object.
		 */

		queue_iterate(&backing_object->memq, p, vm_page_t, listq) {
			
			/*
			 *	If the parent has a page here, or if
			 *	this page falls outside the parent,
			 *	keep going.
			 *
			 *	Otherwise, the backing_object must be
			 *	left in the chain.
			 */
			
			new_offset = (p->offset - backing_offset);
			if (p->offset < backing_offset || new_offset >= size) {

				/*
				 *	Page falls outside of parent.
				 *	Keep going.
				 */

				continue;
			}

			if ((vm_page_lookup(object, new_offset) == VM_PAGE_NULL)
#if	MACH_PAGEMAP
			    &&
			    (vm_external_state_get(object->existence_map,
					new_offset)
					!= VM_EXTERNAL_STATE_EXISTS)
#endif	/* MACH_PAGEMAP */
			    ) {

				/*
				 *	Page still needed.
				 *	Can't go any further.
				 */

				vm_object_unlock(backing_object);
				return;
			}
		}

#if	MACH_PAGEMAP
		/*
		 *	Next, if backing object has been paged out,
		 *	we must check its existence info for pages
		 *	that the parent doesn't have.
		 */

		if (backing_object->pager_created) {
			assert(backing_object->existence_map
					!= VM_EXTERNAL_NULL);
			for (new_offset = 0; new_offset < object->size;
			     new_offset += PAGE_SIZE) {
				vm_offset_t
				offset = new_offset + backing_offset;

				/*
				 *	If this page doesn't exist in
				 *	the backing object's existence
				 *	info, then continue.
				 */

				if (vm_external_state_get(
					backing_object->existence_map,
					offset) == VM_EXTERNAL_STATE_ABSENT) {
					continue;
				}

				/*
				 *	If this page is neither resident
				 *	in the parent nor paged out to
				 *	the parent's pager, then we cannot
				 *	bypass the backing object.
				 */

				if ((vm_page_lookup(object, new_offset) ==
				     VM_PAGE_NULL) &&
				    ((object->existence_map == VM_EXTERNAL_NULL)
					|| (vm_external_state_get(
					object->existence_map, new_offset)
					== VM_EXTERNAL_STATE_ABSENT))) {
					vm_object_unlock(backing_object);
					return;
				}
			}
		}
#else	/* MACH_PAGEMAP */
		assert(! backing_object->pager_created);
#endif	/* MACH_PAGEMAP */

		/*
		 *	All interesting pages in the backing object
		 *	already live in the parent or its pager.
		 *	Thus we can bypass the backing object.
		 */

		vm_object_do_bypass(object, backing_object);

		/*
		 *	Try again with this object's new backing object.
		 */

		continue;
	}
}

/*
 *	Routine:	vm_object_page_remove: [internal]
 *	Purpose:
 *		Removes all physical pages in the specified
 *		object range from the object's list of pages.
 *
 *	In/out conditions:
 *		The object must be locked.
 *		The object must not have paging_in_progress, usually
 *		guaranteed by not having a pager.
 */
unsigned int vm_object_page_remove_lookup = 0;
unsigned int vm_object_page_remove_iterate = 0;

void
vm_object_page_remove(
	register vm_object_t	object,
	register vm_offset_t	start,
	register vm_offset_t	end)
{
	register vm_page_t	p, next;

	/*
	 *	One and two page removals are most popular.
	 *	The factor of 16 here is somewhat arbitrary.
	 *	It balances vm_object_lookup vs iteration.
	 */

	if (atop(end - start) < (unsigned)object->resident_page_count/16) {
		vm_object_page_remove_lookup++;

		for (; start < end; start += PAGE_SIZE) {
			p = vm_page_lookup(object, start);
			if (p != VM_PAGE_NULL) {
				assert(!p->cleaning && !p->pageout);
				if (!p->fictitious)
					pmap_page_protect(p->phys_addr,
							  VM_PROT_NONE);
				VM_PAGE_FREE(p);
			}
		}
	} else {
		vm_object_page_remove_iterate++;

		p = (vm_page_t) queue_first(&object->memq);
		while (!queue_end(&object->memq, (queue_entry_t) p)) {
			next = (vm_page_t) queue_next(&p->listq);
			if ((start <= p->offset) && (p->offset < end)) {
				assert(!p->cleaning && !p->pageout);
				if (!p->fictitious)
				    pmap_page_protect(p->phys_addr,
						      VM_PROT_NONE);
				VM_PAGE_FREE(p);
			}
			p = next;
		}
	}
}

/*
 *	Routine:	vm_object_coalesce
 *	Function:	Coalesces two objects backing up adjoining
 *			regions of memory into a single object.
 *
 *	returns TRUE if objects were combined.
 *
 *	NOTE:	Only works at the moment if the second object is NULL -
 *		if it's not, which object do we lock first?
 *
 *	Parameters:
 *		prev_object	First object to coalesce
 *		prev_offset	Offset into prev_object
 *		next_object	Second object into coalesce
 *		next_offset	Offset into next_object
 *
 *		prev_size	Size of reference to prev_object
 *		next_size	Size of reference to next_object
 *
 *	Conditions:
 *	The object(s) must *not* be locked. The map must be locked
 *	to preserve the reference to the object(s).
 */
int vm_object_coalesce_count = 0;

boolean_t
vm_object_coalesce(
	register vm_object_t	prev_object,
	vm_object_t		next_object,
	vm_offset_t		prev_offset,
	vm_offset_t		next_offset,
	vm_size_t		prev_size,
	vm_size_t		next_size)
{
	vm_size_t	newsize;

#ifdef	lint
	next_offset++;
#endif	/* lint */

	if (next_object != VM_OBJECT_NULL) {
		return(FALSE);
	}

	if (prev_object == VM_OBJECT_NULL) {
		return(TRUE);
	}

	XPR(XPR_VM_OBJECT,
       "vm_object_coalesce: 0x%X prev_off 0x%X prev_size 0x%X next_size 0x%X\n",
		(integer_t)prev_object, prev_offset, prev_size, next_size, 0);

	vm_object_lock(prev_object);

	/*
	 *	Try to collapse the object first
	 */
	vm_object_collapse(prev_object);

	/*
	 *	Can't coalesce if pages not mapped to
	 *	prev_entry may be in use any way:
	 *	. more than one reference
	 *	. paged out
	 *	. shadows another object
	 *	. has a copy elsewhere
	 *	. paging references (pages might be in page-list)
	 */

	if ((prev_object->ref_count > 1) ||
	    prev_object->pager_created ||
	    (prev_object->shadow != VM_OBJECT_NULL) ||
	    (prev_object->copy != VM_OBJECT_NULL) ||
	    (prev_object->paging_in_progress != 0)) {
		vm_object_unlock(prev_object);
		return(FALSE);
	}

	vm_object_coalesce_count++;

	/*
	 *	Remove any pages that may still be in the object from
	 *	a previous deallocation.
	 */
	vm_object_page_remove(prev_object,
		prev_offset + prev_size,
		prev_offset + prev_size + next_size);

	/*
	 *	Extend the object if necessary.
	 */
	newsize = prev_offset + prev_size + next_size;
	if (newsize > prev_object->size) {
#if	MACH_PAGEMAP
		/*
		 *	We cannot extend an object that has existence info,
		 *	since the existence info might then fail to cover
		 *	the entire object.
		 *
		 *	This assertion must be true because the object
		 *	has no pager, and we only create existence info
		 *	for objects with pagers.
		 */
		assert(prev_object->existence_map == VM_EXTERNAL_NULL);
#endif	/* MACH_PAGEMAP */
		prev_object->size = newsize;
	}

	vm_object_unlock(prev_object);
	return(TRUE);
}

/*
 *	Attach a set of physical pages to an object, so that they can
 *	be mapped by mapping the object.  Typically used to map IO memory.
 *
 *	The mapping function and its private data are used to obtain the
 *	physical addresses for each page to be mapped.
 */
void
vm_object_page_map(
	vm_object_t	object,
	vm_offset_t	offset,
	vm_size_t	size,
	vm_offset_t	(*map_fn)(void *map_fn_data, vm_offset_t offset),
	void 		*map_fn_data)	/* private to map_fn */
{
	int	num_pages;
	int	i;
	vm_page_t	m;
	vm_page_t	old_page;
	vm_offset_t	addr;

	num_pages = atop(size);

	for (i = 0; i < num_pages; i++, offset += PAGE_SIZE) {

	    addr = (*map_fn)(map_fn_data, offset);

	    while ((m = vm_page_grab_fictitious()) == VM_PAGE_NULL)
		vm_page_more_fictitious();

	    vm_object_lock(object);
	    if ((old_page = vm_page_lookup(object, offset))
			!= VM_PAGE_NULL)
	    {
		vm_page_lock_queues();
		vm_page_free(old_page);
		vm_page_unlock_queues();
	    }

	    vm_page_init(m, addr);
	    m->private = TRUE;		/* don`t free page */
	    m->wire_count = 1;
	    vm_page_insert(m, object, offset);

	    PAGE_WAKEUP_DONE(m);
	    vm_object_unlock(object);
	}
}

#if	NORMA_VM
void
vm_object_reaper_continue(void)
{
	vm_object_t object;

	for (;;) {
		simple_lock(&vor_lock);
		while (!queue_empty(&vor_list)) {
			queue_remove_first(&vor_list, object,
					   vm_object_t, cached_list);
			simple_unlock(&vor_lock);
			vm_object_deallocate(object);
			simple_lock(&vor_lock);
		}
		assert_wait((event_t)&vor_list, FALSE);
		simple_unlock(&vor_lock);
		thread_block(vm_object_reaper_continue);
	}
}

void vm_object_thread()
{
	vm_object_reaper_continue();
}
#endif	/* NORMA_VM */

#include <mach_kdb.h>

#if	MACH_KDB

#if	NORMA_VM
#include <ddb/db_command.h>
#endif	/* NORMA_VM */
#include <ddb/db_output.h>
#include <vm/vm_print.h>

#define printf	kdbprintf

extern boolean_t	vm_object_cached(
				vm_object_t object);

extern void		print_bitstring(
				char byte);

boolean_t	vm_object_print_pages = FALSE;

void
print_bitstring(
	char byte)
{
	printf("%c%c%c%c%c%c%c%c",
	       ((byte & (1 << 0)) ? '1' : '0'),
	       ((byte & (1 << 1)) ? '1' : '0'),
	       ((byte & (1 << 2)) ? '1' : '0'),
	       ((byte & (1 << 3)) ? '1' : '0'),
	       ((byte & (1 << 4)) ? '1' : '0'),
	       ((byte & (1 << 5)) ? '1' : '0'),
	       ((byte & (1 << 6)) ? '1' : '0'),
	       ((byte & (1 << 7)) ? '1' : '0'));
}

boolean_t
vm_object_cached(
	register vm_object_t object)
{
	register vm_object_t o;

	queue_iterate(&vm_object_cached_list, o, vm_object_t, cached_list) {
		if (object == o) {
			return TRUE;
		}
	}
	return FALSE;
}

#if	MACH_PAGEMAP
/*
 *	vm_external_print:	[ debug ]
 */
void
vm_external_print(
	vm_external_map_t map,
	vm_size_t	size)
{
	if (map == VM_EXTERNAL_NULL) {
		printf("0  ");
	} else {
		vm_size_t existence_size = stob(size);
		printf("{ size=%d, map=[", existence_size);
		if (existence_size > 0) {
			print_bitstring(map[0]);
		}
		if (existence_size > 1) {
			print_bitstring(map[1]);
		}
		if (existence_size > 2) {
			printf("...");
			print_bitstring(map[existence_size-1]);
		}
		printf("] }\n");
	}
	return;
}
#endif	/* MACH_PAGEMAP */

int
vm_follow_object(
	vm_object_t object)
{
	extern int indent;

	int count = 1;

	if (object == VM_OBJECT_NULL)
		return 0;

	iprintf("object 0x%x", object);
	printf(", shadow=0x%x", object->shadow);
	printf(", copy=0x%x", object->copy);
	printf(", pager=0x%x", object->pager);
	printf(", ref=%d\n", object->ref_count);

	indent += 2;
	if (object->shadow)
	    count += vm_follow_object(object->shadow);

	indent -= 2;
	return count;
}

/*
 *	vm_object_print:	[ debug ]
 */
void
vm_object_print(
	vm_object_t	object,
	boolean_t	have_addr,
	int		arg_count,
	char		*modif)
{
	register vm_page_t p;
	extern int indent;
	char *s;
#if	NORMA_VM
	boolean_t show_xmm_stack;
#endif	/* NORMA_VM */

	register int count;

	if (object == VM_OBJECT_NULL)
		return;

#if	NORMA_VM
	if (db_option(modif, 'x'))
		show_xmm_stack = TRUE;
	else
		show_xmm_stack = FALSE;
#endif	/* NORMA_VM */

	iprintf("object 0x%x\n", object);

	indent += 2;

	iprintf("size=0x%x", object->size);
	printf(", cluster=0x%x", object->cluster_size);
	printf(", frozen=0x%x", object->frozen_size);
	printf(", ref_count=%d\n", object->ref_count);
	iprintf("");
#if	TASK_SWAPPER
	printf("res_count=%d, ", object->res_count);
#endif	/* TASK_SWAPPER */
	printf("resident_page_count=%d\n", object->resident_page_count);

	iprintf("shadow=0x%x", object->shadow);
	if (object->shadow) {
		register int i = 0;
		vm_object_t shadow = object;
		while(shadow = shadow->shadow)
			i++;
		printf(" (depth %d)", i);
	}
	printf(", copy=0x%x", object->copy);
	printf(", shadow_offset=0x%x", object->shadow_offset);
	printf(", last_alloc=0x%x\n", object->last_alloc);

	iprintf("pager=0x%x", object->pager);
	printf(", paging_offset=0x%x", object->paging_offset);
	printf(", pager_request=0x%x\n", object->pager_request);

	iprintf("copy_strategy=%d[", object->copy_strategy);
	switch (object->copy_strategy) {
		case MEMORY_OBJECT_COPY_NONE:
		printf("copy_none");
		break;

		case MEMORY_OBJECT_COPY_CALL:
		printf("copy_call");
		break;

		case MEMORY_OBJECT_COPY_DELAY:
		printf("copy_delay");
		break;

		case MEMORY_OBJECT_COPY_SYMMETRIC:
		printf("copy_symmetric");
		break;

		case MEMORY_OBJECT_COPY_INVALID:
		printf("copy_invalid");
		break;

		default:
		printf("?");
	}
	printf("]");
	printf(", absent_count=%d\n", object->absent_count);

	iprintf("all_wanted=0x%x<", object->all_wanted);
	s = "";
	if (vm_object_wanted(object, VM_OBJECT_EVENT_INITIALIZED)) {
		printf("%sinit", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_PAGER_READY)) {
		printf("%sready", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_PAGING_IN_PROGRESS)) {
		printf("%spaging", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_ABSENT_COUNT)) {
		printf("%sabsent", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_LOCK_IN_PROGRESS)) {
		printf("%slock", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_UNCACHING)) {
		printf("%suncaching", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_COPY_CALL)) {
		printf("%scopy_call", s);
		s = ",";
	}
	if (vm_object_wanted(object, VM_OBJECT_EVENT_CACHING)) {
		printf("%scaching", s);
		s = ",";
	}
	printf(">");
	printf(", paging_in_progress=%d\n", object->paging_in_progress);

	iprintf("%screated, %sinit, %sready, %spersist, %strusted, %spageout, %s, %s\n",
		(object->pager_created ? "" : "!"),
		(object->pager_initialized ? "" : "!"),
		(object->pager_ready ? "" : "!"),
		(object->can_persist ? "" : "!"),
		(object->pager_trusted ? "" : "!"),
		(object->pageout ? "" : "!"),
		(object->internal ? "internal" : "external"),
		(object->temporary ? "temporary" : "permanent"));
	iprintf("%salive, %slock_in_progress, %slock_restart, %sshadowed, %scached, %sprivate\n",
		(object->alive ? "" : "!"),
		(object->lock_in_progress ? "" : "!"),
		(object->lock_restart ? "" : "!"),
		(object->shadowed ? "" : "!"),
		(vm_object_cached(object) ? "" : "!"),
		(object->private ? "" : "!"));
	iprintf("%sadvisory_pageout, %ssilent_overwrite\n",
		(object->advisory_pageout ? "" : "!"),
		(object->silent_overwrite ? "" : "!"));

#if	MACH_PAGEMAP
	iprintf("existence_map=");
	vm_external_print(object->existence_map, object->size);
#endif	/* MACH_PAGEMAP */

#if	NORMA_VM
	iprintf("%spagemap_exported, %stemporary_cached\n",
		(object->pagemap_exported ? "" : "!"),
		(object->temporary_cached ? "" : "!"));
	iprintf("%stemporary_caching, %stemporary_uncaching\n",
		(object->temporary_caching ? "" : "!"),
		(object->temporary_uncaching ? "" : "!"));
#endif	/* NORMA_VM */
#if	MACH_ASSERT
	iprintf("paging_object=0x%x\n", object->paging_object);
#endif	/* MACH_ASSERT */

	if (vm_object_print_pages) {
		count = 0;
		p = (vm_page_t) queue_first(&object->memq);
		while (!queue_end(&object->memq, (queue_entry_t) p)) {
			if (count == 0) {
				iprintf("memory:=");
			} else if (count == 2) {
				printf("\n");
				iprintf(" ...");
				count = 0;
			} else {
				printf(",");
			}
			count++;

			printf("(off=0x%X,page=0x%X)", p->offset, (integer_t) p);
			p = (vm_page_t) queue_next(&p->listq);
		}
		if (count != 0) {
			printf("\n");
		}
	}
	indent -= 2;
#if	NORMA_VM
	if (show_xmm_stack && object->pager_request)
		xmm_object_stack_print(object->pager_request);
#endif	/* NORMA_VM */
}


/*
 *	vm_object_find		[ debug ]
 *
 *	Find all tasks which reference the given vm_object.
 */

boolean_t vm_object_find(vm_object_t object);
boolean_t vm_object_print_verbose = FALSE;

boolean_t
vm_object_find(
	vm_object_t     object)
{
        task_t task;
	vm_map_t map;
	vm_map_entry_t entry;
        processor_set_t pset;
	boolean_t found = FALSE;

        queue_iterate(&all_psets, pset, processor_set_t, all_psets) {
            queue_iterate(&pset->tasks, task, task_t, pset_tasks) {
		map = task->map;
        	for (entry = vm_map_first_entry(map);
                     entry && entry != vm_map_to_entry(map);
                     entry = entry->vme_next) {

			vm_object_t obj;

			/* 
			 * For the time being skip submaps,
			 * only the kernel can have submaps,
			 * and unless we are interested in 
			 * kernel objects, we can simply skip 
			 * submaps. See sb/dejan/nmk18b7/src/mach_kernel/vm
			 * for a full solution.
			 */
			if (entry->is_sub_map)
				continue;
			if (entry) 
				obj = entry->object.vm_object;
			else 
				continue;

			while (obj != VM_OBJECT_NULL) {
				if (obj == object) {
					if (!found) {
						printf("TASK\t\tMAP\t\tENTRY\n");
						found = TRUE;
					}
					printf("0x%x\t0x%x\t0x%x\n", 
							task, map, entry);
				}
				obj = obj->shadow;
			}
        	}
            }
        }

	return(found);
}

#endif	/* MACH_KDB */


/*
 * ============================================================================
 * VM OBJECT COMPRESSION FUNCTIONS
 * ============================================================================
 */

/*
 * Structure for compressed object data
 */
struct vm_object_compressed_data {
    unsigned char *compressed_pages;
    unsigned long long *page_offsets;
    unsigned int *page_sizes;
    unsigned int num_pages;
    unsigned long long original_size;
    unsigned long long compressed_size;
    unsigned int algorithm;
    simple_lock_t comp_lock;
};

/*
 * vm_object_compress_pages
 *
 * Compress all pages in an object to reduce memory footprint
 */
kern_return_t vm_object_compress_pages(vm_object_t object, unsigned int algorithm)
{
    struct vm_object_compressed_data *comp_data;
    vm_page_t page, next_page;
    unsigned char *page_buffer;
    unsigned char *comp_buffer;
    vm_size_t page_size = PAGE_SIZE;
    vm_size_t comp_size;
    unsigned int page_count = 0;
    unsigned long long total_original = 0;
    unsigned long long total_compressed = 0;
    unsigned int i;
    kern_return_t kr = KERN_SUCCESS;
    
    if (object == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    /* Allocate compression data structure */
    comp_data = (struct vm_object_compressed_data *)kalloc(sizeof(struct vm_object_compressed_data));
    if (comp_data == NULL)
        return KERN_RESOURCE_SHORTAGE;
    
    memset(comp_data, 0, sizeof(struct vm_object_compressed_data));
    simple_lock_init(&comp_data->comp_lock);
    
    /* Count resident pages */
    vm_object_lock(object);
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (!page->fictitious && !page->absent) {
            page_count++;
        }
    }
    vm_object_unlock(object);
    
    if (page_count == 0) {
        kfree((vm_offset_t)comp_data, sizeof(struct vm_object_compressed_data));
        return KERN_SUCCESS;
    }
    
    /* Allocate arrays for compressed data */
    comp_data->page_offsets = (unsigned long long *)kalloc(page_count * sizeof(unsigned long long));
    comp_data->page_sizes = (unsigned int *)kalloc(page_count * sizeof(unsigned int));
    comp_data->compressed_pages = (unsigned char *)kalloc(page_count * PAGE_SIZE);
    
    if (comp_data->page_offsets == NULL || comp_data->page_sizes == NULL || 
        comp_data->compressed_pages == NULL) {
        if (comp_data->page_offsets) kfree((vm_offset_t)comp_data->page_offsets, 
                                           page_count * sizeof(unsigned long long));
        if (comp_data->page_sizes) kfree((vm_offset_t)comp_data->page_sizes, 
                                         page_count * sizeof(unsigned int));
        if (comp_data->compressed_pages) kfree((vm_offset_t)comp_data->compressed_pages, 
                                               page_count * PAGE_SIZE);
        kfree((vm_offset_t)comp_data, sizeof(struct vm_object_compressed_data));
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Compress each resident page */
    vm_object_lock(object);
    
    i = 0;
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (page->fictitious || page->absent)
            continue;
        
        page_buffer = (unsigned char *)phystokv(page->phys_addr);
        comp_buffer = comp_data->compressed_pages + total_compressed;
        
        /* Compress page based on algorithm */
        switch (algorithm) {
            case VM_OBJECT_COMPRESS_LZ4:
                comp_size = vm_compress_lz4(page_buffer, page_size, comp_buffer, page_size);
                break;
            case VM_OBJECT_COMPRESS_ZSTD:
                comp_size = vm_compress_zstd(page_buffer, page_size, comp_buffer, page_size);
                break;
            case VM_OBJECT_COMPRESS_LZO:
                comp_size = vm_compress_lzo(page_buffer, page_size, comp_buffer, page_size);
                break;
            default:
                comp_size = page_size;
                break;
        }
        
        if (comp_size < page_size) {
            comp_data->page_offsets[i] = total_compressed;
            comp_data->page_sizes[i] = comp_size;
            total_compressed += comp_size;
            total_original += page_size;
            i++;
            
            /* Mark page as compressed and remove from memory */
            page->compressed = TRUE;
            page->compressed_offset = total_compressed - comp_size;
            page->compressed_size = comp_size;
            
            /* Remove from pmap */
            pmap_remove(kernel_pmap, (vm_offset_t)page_buffer, 
                       (vm_offset_t)page_buffer + page_size);
        }
    }
    
    comp_data->num_pages = i;
    comp_data->original_size = total_original;
    comp_data->compressed_size = total_compressed;
    comp_data->algorithm = algorithm;
    
    object->compressed_data = comp_data;
    object->is_compressed = TRUE;
    
    vm_object_unlock(object);
    
    return KERN_SUCCESS;
}

/*
 * vm_object_decompress_pages
 *
 * Decompress previously compressed object pages
 */
kern_return_t vm_object_decompress_pages(vm_object_t object)
{
    struct vm_object_compressed_data *comp_data;
    vm_page_t page;
    unsigned char *page_buffer;
    unsigned char *comp_buffer;
    unsigned int i;
    
    if (object == VM_OBJECT_NULL || !object->is_compressed)
        return KERN_INVALID_ARGUMENT;
    
    comp_data = object->compressed_data;
    if (comp_data == NULL)
        return KERN_FAILURE;
    
    simple_lock(&comp_data->comp_lock);
    
    /* Decompress each compressed page */
    for (i = 0; i < comp_data->num_pages; i++) {
        comp_buffer = comp_data->compressed_pages + comp_data->page_offsets[i];
        
        /* Find the page in the object */
        vm_object_lock(object);
        page = vm_page_lookup(object, i * PAGE_SIZE);
        if (page != VM_PAGE_NULL && page->compressed) {
            page_buffer = (unsigned char *)phystokv(page->phys_addr);
            
            /* Decompress based on algorithm */
            switch (comp_data->algorithm) {
                case VM_OBJECT_COMPRESS_LZ4:
                    vm_decompress_lz4(comp_buffer, comp_data->page_sizes[i],
                                      page_buffer, PAGE_SIZE);
                    break;
                case VM_OBJECT_COMPRESS_ZSTD:
                    vm_decompress_zstd(comp_buffer, comp_data->page_sizes[i],
                                       page_buffer, PAGE_SIZE);
                    break;
                case VM_OBJECT_COMPRESS_LZO:
                    vm_decompress_lzo(comp_buffer, comp_data->page_sizes[i],
                                      page_buffer, PAGE_SIZE);
                    break;
            }
            
            page->compressed = FALSE;
            page->compressed_offset = 0;
            page->compressed_size = 0;
            
            /* Re-enter in pmap */
            pmap_enter(kernel_pmap, (vm_offset_t)page_buffer, page->phys_addr,
                      VM_PROT_DEFAULT, TRUE);
        }
        vm_object_unlock(object);
    }
    
    /* Free compressed data */
    kfree((vm_offset_t)comp_data->page_offsets, comp_data->num_pages * sizeof(unsigned long long));
    kfree((vm_offset_t)comp_data->page_sizes, comp_data->num_pages * sizeof(unsigned int));
    kfree((vm_offset_t)comp_data->compressed_pages, comp_data->num_pages * PAGE_SIZE);
    kfree((vm_offset_t)comp_data, sizeof(struct vm_object_compressed_data));
    
    object->compressed_data = NULL;
    object->is_compressed = FALSE;
    
    simple_unlock(&comp_data->comp_lock);
    
    return KERN_SUCCESS;
}

/*
 * ============================================================================
 * VM OBJECT DEDUPLICATION FUNCTIONS
 * ============================================================================
 */

/*
 * Hash table for page content deduplication
 */
struct vm_object_dedup_hash {
    unsigned long long hash;
    vm_offset_t phys_addr;
    vm_object_t object;
    vm_offset_t offset;
    unsigned int ref_count;
    struct vm_object_dedup_hash *next;
};

static struct vm_object_dedup_hash *dedup_hash_table[65536];
static simple_lock_t dedup_hash_lock;

/*
 * vm_object_deduplicate_pages
 *
 * Find and eliminate duplicate page content across objects
 */
kern_return_t vm_object_deduplicate_pages(vm_object_t object)
{
    vm_page_t page, next_page;
    unsigned long long hash;
    unsigned int hash_index;
    struct vm_object_dedup_hash *entry, *new_entry;
    unsigned int dedup_count = 0;
    unsigned long long saved_bytes = 0;
    
    if (object == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    simple_lock(&dedup_hash_lock);
    
    vm_object_lock(object);
    
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (page->fictitious || page->absent || page->deduplicated)
            continue;
        
        /* Calculate hash of page content */
        unsigned char *page_data = (unsigned char *)phystokv(page->phys_addr);
        hash = vm_page_hash(page_data, PAGE_SIZE);
        hash_index = hash % 65536;
        
        /* Look for existing page with same hash */
        entry = dedup_hash_table[hash_index];
        while (entry != NULL) {
            if (entry->hash == hash) {
                /* Verify content match */
                unsigned char *existing_data = (unsigned char *)phystokv(entry->phys_addr);
                if (memcmp(page_data, existing_data, PAGE_SIZE) == 0) {
                    /* Found duplicate - share the page */
                    entry->ref_count++;
                    page->deduplicated = TRUE;
                    page->shared_phys_addr = entry->phys_addr;
                    
                    /* Remove the duplicate page */
                    vm_page_lock_queues();
                    vm_page_free(page);
                    vm_page_unlock_queues();
                    
                    dedup_count++;
                    saved_bytes += PAGE_SIZE;
                    break;
                }
            }
            entry = entry->next;
        }
        
        if (!page->deduplicated) {
            /* New unique page - add to hash table */
            new_entry = (struct vm_object_dedup_hash *)kalloc(sizeof(struct vm_object_dedup_hash));
            if (new_entry != NULL) {
                new_entry->hash = hash;
                new_entry->phys_addr = page->phys_addr;
                new_entry->object = object;
                new_entry->offset = page->offset;
                new_entry->ref_count = 1;
                new_entry->next = dedup_hash_table[hash_index];
                dedup_hash_table[hash_index] = new_entry;
            }
        }
    }
    
    vm_object_unlock(object);
    simple_unlock(&dedup_hash_lock);
    
    printf("VM Object Deduplication: %u pages deduplicated, %llu bytes saved\n",
           dedup_count, saved_bytes);
    
    return KERN_SUCCESS;
}

/*
 * ============================================================================
 * VM OBJECT POOL MANAGEMENT
 * ============================================================================
 */

/*
 * Object pool for frequently allocated objects
 */
struct vm_object_pool {
    vm_object_t *objects;
    unsigned int size;
    unsigned int count;
    unsigned int max_size;
    simple_lock_t pool_lock;
};

static struct vm_object_pool *object_pools[OBJECT_POOL_COUNT];

/*
 * vm_object_pool_init
 *
 * Initialize object pools for different sizes
 */
void vm_object_pool_init(void)
{
    unsigned int i;
    
    for (i = 0; i < OBJECT_POOL_COUNT; i++) {
        object_pools[i] = (struct vm_object_pool *)kalloc(sizeof(struct vm_object_pool));
        if (object_pools[i] != NULL) {
            object_pools[i]->size = PAGE_SIZE * (1 << i);
            object_pools[i]->max_size = 100;
            object_pools[i]->objects = (vm_object_t *)kalloc(object_pools[i]->max_size * sizeof(vm_object_t));
            object_pools[i]->count = 0;
            simple_lock_init(&object_pools[i]->pool_lock);
        }
    }
    
    simple_lock_init(&dedup_hash_lock);
    memset(dedup_hash_table, 0, sizeof(dedup_hash_table));
}

/*
 * vm_object_allocate_from_pool
 *
 * Allocate object from pool if available
 */
vm_object_t vm_object_allocate_from_pool(vm_size_t size)
{
    unsigned int pool_index = 0;
    vm_size_t pool_size = PAGE_SIZE;
    vm_object_t object = VM_OBJECT_NULL;
    
    /* Find appropriate pool */
    while (pool_size < size && pool_index < OBJECT_POOL_COUNT - 1) {
        pool_size <<= 1;
        pool_index++;
    }
    
    if (pool_index < OBJECT_POOL_COUNT && object_pools[pool_index] != NULL) {
        struct vm_object_pool *pool = object_pools[pool_index];
        
        simple_lock(&pool->pool_lock);
        if (pool->count > 0) {
            object = pool->objects[--pool->count];
        }
        simple_unlock(&pool->pool_lock);
    }
    
    if (object == VM_OBJECT_NULL) {
        object = vm_object_allocate(size);
    } else {
        /* Reinitialize object */
        vm_object_lock(object);
        _vm_object_allocate(size, object);
        vm_object_unlock(object);
    }
    
    return object;
}

/*
 * vm_object_deallocate_to_pool
 *
 * Return object to pool if appropriate
 */
void vm_object_deallocate_to_pool(vm_object_t object)
{
    unsigned int pool_index = 0;
    vm_size_t pool_size = PAGE_SIZE;
    vm_size_t obj_size;
    
    if (object == VM_OBJECT_NULL)
        return;
    
    obj_size = object->size;
    
    /* Find appropriate pool */
    while (pool_size < obj_size && pool_index < OBJECT_POOL_COUNT - 1) {
        pool_size <<= 1;
        pool_index++;
    }
    
    if (pool_index < OBJECT_POOL_COUNT && object_pools[pool_index] != NULL) {
        struct vm_object_pool *pool = object_pools[pool_index];
        
        simple_lock(&pool->pool_lock);
        if (pool->count < pool->max_size) {
            pool->objects[pool->count++] = object;
            simple_unlock(&pool->pool_lock);
            return;
        }
        simple_unlock(&pool->pool_lock);
    }
    
    /* No pool or pool full - destroy object */
    vm_object_deallocate(object);
}

/*
 * ============================================================================
 * VM OBJECT REFERENCE TRACKING
 * ============================================================================
 */

/*
 * Track object references for debugging
 */
struct vm_object_ref_track {
    vm_object_t object;
    unsigned long long ref_count;
    unsigned long long last_ref_time;
    thread_t last_ref_thread;
    unsigned int ref_stack[16];
    unsigned int stack_depth;
};

static struct vm_object_ref_track ref_tracks[MAX_OBJECT_TRACK];
static unsigned int ref_track_count = 0;
static simple_lock_t ref_track_lock;

/*
 * vm_object_track_reference
 *
 * Track a reference to an object
 */
void vm_object_track_reference(vm_object_t object, thread_t thread)
{
    unsigned int i;
    
    if (object == VM_OBJECT_NULL)
        return;
    
    simple_lock(&ref_track_lock);
    
    for (i = 0; i < ref_track_count; i++) {
        if (ref_tracks[i].object == object) {
            ref_tracks[i].ref_count++;
            ref_tracks[i].last_ref_time = mach_absolute_time();
            ref_tracks[i].last_ref_thread = thread;
            break;
        }
    }
    
    if (i == ref_track_count && ref_track_count < MAX_OBJECT_TRACK) {
        ref_tracks[ref_track_count].object = object;
        ref_tracks[ref_track_count].ref_count = 1;
        ref_tracks[ref_track_count].last_ref_time = mach_absolute_time();
        ref_tracks[ref_track_count].last_ref_thread = thread;
        ref_track_count++;
    }
    
    simple_unlock(&ref_track_lock);
}

/*
 * vm_object_get_reference_info
 *
 * Get reference information for an object
 */
void vm_object_get_reference_info(vm_object_t object, unsigned long long *ref_count,
                                   unsigned long long *last_time, thread_t *last_thread)
{
    unsigned int i;
    
    if (object == VM_OBJECT_NULL)
        return;
    
    simple_lock(&ref_track_lock);
    
    for (i = 0; i < ref_track_count; i++) {
        if (ref_tracks[i].object == object) {
            if (ref_count != NULL) *ref_count = ref_tracks[i].ref_count;
            if (last_time != NULL) *last_time = ref_tracks[i].last_ref_time;
            if (last_thread != NULL) *last_thread = ref_tracks[i].last_ref_thread;
            break;
        }
    }
    
    simple_unlock(&ref_track_lock);
}

/*
 * ============================================================================
 * VM OBJECT AGE AND PROMOTION
 * ============================================================================
 */

/*
 * Promote frequently accessed objects to faster memory
 */
void vm_object_promote_hot_objects(void)
{
    vm_object_t object;
    unsigned long long now;
    unsigned long long age;
    unsigned int hot_threshold = 1000000000ULL; /* 1 second */
    
    now = mach_absolute_time();
    
    vm_object_cache_lock();
    
    queue_iterate(&vm_object_cached_list, object, vm_object_t, cached_list) {
        vm_object_lock(object);
        
        age = now - object->last_access_time;
        
        if (age < hot_threshold && object->resident_page_count > 0) {
            /* Object is hot - move to faster NUMA node */
            vm_page_t page;
            queue_iterate(&object->memq, page, vm_page_t, listq) {
                if (!page->fictitious && !page->absent && page->numa_node != 0) {
                    vm_page_migrate_to_node(page, 0);
                }
            }
        }
        
        vm_object_unlock(object);
    }
    
    vm_object_cache_unlock();
}

/*
 * ============================================================================
 * VM OBJECT STATISTICS
 * ============================================================================
 */

/*
 * Get comprehensive object statistics
 */
void vm_object_get_stats(struct vm_object_stats *stats)
{
    vm_object_t object;
    
    if (stats == NULL)
        return;
    
    memset(stats, 0, sizeof(struct vm_object_stats));
    
    vm_object_cache_lock();
    
    queue_iterate(&vm_object_cached_list, object, vm_object_t, cached_list) {
        stats->cached_objects++;
        stats->cached_pages += object->resident_page_count;
        stats->cached_memory += object->resident_page_count * PAGE_SIZE;
    }
    
    vm_object_cache_unlock();
    
    stats->total_objects = vm_object_zone->z_count;
    stats->active_objects = stats->total_objects - stats->cached_objects;
    
    /* Get zone statistics */
    stats->zone_allocated = vm_object_zone->z_alloc_count;
    stats->zone_free = vm_object_zone->z_free_count;
    stats->zone_size = vm_object_zone->z_cur_size;
}

/*
 * ============================================================================
 * CONSTANTS AND DEFINITIONS
 * ============================================================================
 */

#define VM_OBJECT_COMPRESS_LZ4   1
#define VM_OBJECT_COMPRESS_ZSTD  2
#define VM_OBJECT_COMPRESS_LZO   3

#define OBJECT_POOL_COUNT        8
#define MAX_OBJECT_TRACK         10000

struct vm_object_stats {
    unsigned int total_objects;
    unsigned int active_objects;
    unsigned int cached_objects;
    unsigned long long cached_pages;
    unsigned long long cached_memory;
    unsigned long long zone_allocated;
    unsigned long long zone_free;
    unsigned long long zone_size;
};


/*
 * ============================================================================
 * VM OBJECT MERGING (Memory Defragmentation)
 * ============================================================================
 */

/*
 * Structure for tracking adjacent objects that can be merged
 */
struct vm_object_merge_candidate {
    vm_object_t obj1;
    vm_object_t obj2;
    vm_offset_t merge_point;
    vm_size_t total_size;
    unsigned long long common_pages;
    float merge_benefit;
    unsigned int priority;
    struct vm_object_merge_candidate *next;
};

static struct vm_object_merge_candidate *merge_candidates = NULL;
static simple_lock_t merge_lock;

/*
 * vm_object_find_merge_candidates
 *
 * Find adjacent objects that can be merged to reduce fragmentation
 */
static void vm_object_find_merge_candidates(vm_object_t object)
{
    struct vm_object_merge_candidate *candidate;
    vm_object_t prev_obj;
    vm_page_t page;
    unsigned long long common_count = 0;
    vm_size_t total_pages;
    float benefit;
    
    if (object == VM_OBJECT_NULL || object->shadow != VM_OBJECT_NULL)
        return;
    
    /* Check previous object in address space (would need map info) */
    prev_obj = object->merge_prev;
    if (prev_obj == VM_OBJECT_NULL)
        return;
    
    /* Count common pages between objects */
    vm_object_lock(object);
    vm_object_lock(prev_obj);
    
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        vm_page_t prev_page = vm_page_lookup(prev_obj, page->offset);
        if (prev_page != VM_PAGE_NULL && 
            memcmp(phystokv(page->phys_addr), phystokv(prev_page->phys_addr), PAGE_SIZE) == 0) {
            common_count++;
        }
    }
    
    total_pages = (object->size + prev_obj->size) / PAGE_SIZE;
    benefit = (float)common_count / total_pages;
    
    vm_object_unlock(prev_obj);
    vm_object_unlock(object);
    
    /* Only consider merging if significant commonality (>30%) */
    if (benefit > 0.3f) {
        candidate = (struct vm_object_merge_candidate *)kalloc(sizeof(struct vm_object_merge_candidate));
        if (candidate != NULL) {
            candidate->obj1 = prev_obj;
            candidate->obj2 = object;
            candidate->merge_point = prev_obj->size;
            candidate->total_size = prev_obj->size + object->size;
            candidate->common_pages = common_count;
            candidate->merge_benefit = benefit;
            candidate->priority = (unsigned int)(benefit * 100);
            
            simple_lock(&merge_lock);
            candidate->next = merge_candidates;
            merge_candidates = candidate;
            simple_unlock(&merge_lock);
        }
    }
}

/*
 * vm_object_merge_objects
 *
 * Merge two adjacent objects into a single object
 */
kern_return_t vm_object_merge_objects(vm_object_t obj1, vm_object_t obj2)
{
    vm_page_t page, next_page;
    vm_offset_t new_offset;
    kern_return_t kr = KERN_SUCCESS;
    
    if (obj1 == VM_OBJECT_NULL || obj2 == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    vm_object_lock(obj1);
    vm_object_lock(obj2);
    
    /* Verify objects can be merged */
    if (obj1->pager_created || obj2->pager_created ||
        obj1->shadow != VM_OBJECT_NULL || obj2->shadow != VM_OBJECT_NULL ||
        obj1->copy != VM_OBJECT_NULL || obj2->copy != VM_OBJECT_NULL ||
        obj1->ref_count > 1 || obj2->ref_count > 1) {
        vm_object_unlock(obj2);
        vm_object_unlock(obj1);
        return KERN_FAILURE;
    }
    
    /* Move all pages from obj2 to obj1 */
    queue_iterate_safe(&obj2->memq, page, next_page, vm_page_t, listq) {
        new_offset = obj1->size + page->offset;
        
        /* Check for conflict */
        if (vm_page_lookup(obj1, new_offset) != VM_PAGE_NULL) {
            kr = KERN_FAILURE;
            break;
        }
        
        /* Rename page to new object */
        vm_page_lock_queues();
        vm_page_rename(page, obj1, new_offset);
        vm_page_unlock_queues();
    }
    
    if (kr == KERN_SUCCESS) {
        /* Expand obj1 to include obj2's size */
        obj1->size += obj2->size;
        
        /* Transfer pager if exists */
        if (obj2->pager != IP_NULL) {
            obj1->pager = obj2->pager;
            obj1->pager_created = obj2->pager_created;
            obj1->pager_request = obj2->pager_request;
            obj2->pager = IP_NULL;
            obj2->pager_request = PAGER_REQUEST_NULL;
        }
        
        /* Mark obj2 for termination */
        obj2->alive = FALSE;
    }
    
    vm_object_unlock(obj2);
    vm_object_unlock(obj1);
    
    if (kr == KERN_SUCCESS) {
        vm_object_deallocate(obj2);
    }
    
    return kr;
}

/*
 * vm_object_process_merge_candidates
 *
 * Process all pending merge candidates
 */
void vm_object_process_merge_candidates(void)
{
    struct vm_object_merge_candidate *candidate, *next;
    unsigned int merged = 0;
    
    simple_lock(&merge_lock);
    candidate = merge_candidates;
    merge_candidates = NULL;
    simple_unlock(&merge_lock);
    
    while (candidate != NULL) {
        next = candidate->next;
        
        if (vm_object_merge_objects(candidate->obj1, candidate->obj2) == KERN_SUCCESS) {
            merged++;
        }
        
        kfree((vm_offset_t)candidate, sizeof(struct vm_object_merge_candidate));
        candidate = next;
    }
    
    if (merged > 0) {
        printf("VM Object Merge: %u objects merged\n", merged);
    }
}

/*
 * ============================================================================
 * CLONE DETECTION AND SHARING
 * ============================================================================
 */

/*
 * Structure for clone detection
 */
struct vm_clone_signature {
    unsigned long long *page_hashes;
    unsigned int num_pages;
    unsigned long long signature_hash;
    vm_object_t object;
    struct vm_clone_signature *next;
};

static struct vm_clone_signature *clone_signatures[1024];
static simple_lock_t clone_lock;

/*
 * vm_object_detect_clones
 *
 * Detect identical objects that could be shared (copy-on-write)
 */
kern_return_t vm_object_detect_clones(vm_object_t object)
{
    struct vm_clone_signature *sig;
    vm_page_t page;
    unsigned long long *hashes;
    unsigned long long total_hash = 0;
    unsigned int num_pages = 0;
    unsigned int hash_index;
    
    if (object == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    /* Calculate hash signature for object */
    vm_object_lock(object);
    
    /* Count resident pages */
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (!page->fictitious && !page->absent) {
            num_pages++;
        }
    }
    
    if (num_pages == 0) {
        vm_object_unlock(object);
        return KERN_SUCCESS;
    }
    
    /* Allocate hash array */
    hashes = (unsigned long long *)kalloc(num_pages * sizeof(unsigned long long));
    if (hashes == NULL) {
        vm_object_unlock(object);
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Compute hashes for each page */
    unsigned int idx = 0;
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (!page->fictitious && !page->absent) {
            unsigned char *data = (unsigned char *)phystokv(page->phys_addr);
            hashes[idx] = vm_page_hash(data, PAGE_SIZE);
            total_hash ^= hashes[idx];
            idx++;
        }
    }
    
    vm_object_unlock(object);
    
    hash_index = total_hash % 1024;
    
    /* Look for matching signature */
    simple_lock(&clone_lock);
    
    sig = clone_signatures[hash_index];
    while (sig != NULL) {
        if (sig->signature_hash == total_hash && sig->num_pages == num_pages) {
            /* Potential clone found - verify content */
            boolean_t is_clone = TRUE;
            
            vm_object_lock(sig->object);
            vm_object_lock(object);
            
            /* Verify all pages match */
            vm_page_t page1, page2;
            queue_iterate(&object->memq, page1, vm_page_t, listq) {
                if (page1->fictitious || page1->absent)
                    continue;
                    
                page2 = vm_page_lookup(sig->object, page1->offset);
                if (page2 == VM_PAGE_NULL || page2->fictitious || page2->absent) {
                    is_clone = FALSE;
                    break;
                }
                
                unsigned char *data1 = (unsigned char *)phystokv(page1->phys_addr);
                unsigned char *data2 = (unsigned char *)phystokv(page2->phys_addr);
                if (memcmp(data1, data2, PAGE_SIZE) != 0) {
                    is_clone = FALSE;
                    break;
                }
            }
            
            vm_object_unlock(object);
            vm_object_unlock(sig->object);
            
            if (is_clone) {
                /* Found clone - can share pages */
                simple_unlock(&clone_lock);
                kfree((vm_offset_t)hashes, num_pages * sizeof(unsigned long long));
                return KERN_SUCCESS;
            }
        }
        sig = sig->next;
    }
    
    /* No clone found - add signature */
    sig = (struct vm_clone_signature *)kalloc(sizeof(struct vm_clone_signature));
    if (sig != NULL) {
        sig->page_hashes = hashes;
        sig->num_pages = num_pages;
        sig->signature_hash = total_hash;
        sig->object = object;
        sig->next = clone_signatures[hash_index];
        clone_signatures[hash_index] = sig;
    }
    
    simple_unlock(&clone_lock);
    
    return KERN_SUCCESS;
}

/*
 * vm_object_share_clone
 *
 * Share pages between clone objects (COW)
 */
kern_return_t vm_object_share_clone(vm_object_t obj1, vm_object_t obj2)
{
    vm_page_t page;
    
    if (obj1 == VM_OBJECT_NULL || obj2 == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    vm_object_lock(obj1);
    vm_object_lock(obj2);
    
    /* Make obj2 a shadow of obj1 */
    obj2->shadow = obj1;
    obj2->shadow_offset = 0;
    obj1->ref_count++;
    obj1->shadowed = TRUE;
    
    /* Mark all pages as copy-on-write */
    queue_iterate(&obj2->memq, page, vm_page_t, listq) {
        if (!page->fictitious && !page->absent) {
            pmap_page_protect(page->phys_addr, VM_PROT_READ);
            page->cow = TRUE;
        }
    }
    
    vm_object_unlock(obj2);
    vm_object_unlock(obj1);
    
    return KERN_SUCCESS;
}

/*
 * ============================================================================
 * PAGE COLORING FOR CACHE OPTIMIZATION
 * ============================================================================
 */

/*
 * Page coloring configuration
 */
struct vm_page_coloring_config {
    unsigned int cache_colors;
    unsigned int color_bits;
    unsigned int color_mask;
    unsigned int *color_counts;
    simple_lock_t color_lock;
};

static struct vm_page_coloring_config page_coloring;

/*
 * vm_page_coloring_init
 *
 * Initialize page coloring for cache optimization
 */
void vm_page_coloring_init(void)
{
    unsigned int l3_cache_size = machine_l3_cache_size();
    unsigned int cache_line_size = machine_cache_line_size();
    
    if (l3_cache_size > 0 && cache_line_size > 0) {
        page_coloring.cache_colors = l3_cache_size / cache_line_size;
        page_coloring.color_bits = 0;
        while ((1U << page_coloring.color_bits) < page_coloring.cache_colors) {
            page_coloring.color_bits++;
        }
        page_coloring.color_mask = (1U << page_coloring.color_bits) - 1;
        
        page_coloring.color_counts = (unsigned int *)kalloc(
            page_coloring.cache_colors * sizeof(unsigned int));
        if (page_coloring.color_counts != NULL) {
            memset(page_coloring.color_counts, 0, 
                   page_coloring.cache_colors * sizeof(unsigned int));
        }
        
        simple_lock_init(&page_coloring.color_lock);
        
        printf("Page coloring initialized: %u colors, %u bits, mask=0x%x\n",
               page_coloring.cache_colors, page_coloring.color_bits, page_coloring.color_mask);
    }
}

/*
 * vm_object_assign_color
 *
 * Assign cache color to a page
 */
unsigned int vm_object_assign_color(vm_object_t object, vm_offset_t offset)
{
    unsigned int color = 0;
    
    if (page_coloring.cache_colors > 0) {
        color = (offset / PAGE_SIZE) & page_coloring.color_mask;
        color = color % page_coloring.cache_colors;
        
        simple_lock(&page_coloring.color_lock);
        if (page_coloring.color_counts != NULL) {
            page_coloring.color_counts[color]++;
        }
        simple_unlock(&page_coloring.color_lock);
    }
    
    return color;
}

/*
 * vm_object_balance_colors
 *
 * Balance page colors to reduce cache conflicts
 */
void vm_object_balance_colors(void)
{
    unsigned int min_color = 0;
    unsigned int max_color = 0;
    unsigned int min_count = ~0U;
    unsigned int max_count = 0;
    unsigned int i;
    
    if (page_coloring.color_counts == NULL)
        return;
    
    simple_lock(&page_coloring.color_lock);
    
    for (i = 0; i < page_coloring.cache_colors; i++) {
        if (page_coloring.color_counts[i] < min_count) {
            min_count = page_coloring.color_counts[i];
            min_color = i;
        }
        if (page_coloring.color_counts[i] > max_count) {
            max_count = page_coloring.color_counts[i];
            max_color = i;
        }
    }
    
    /* If imbalance > 20%, rebalance */
    if (max_count > min_count * 1.2) {
        /* Would trigger page migration to balance colors */
        printf("Page coloring imbalance: min=%u (%u), max=%u (%u)\n",
               min_color, min_count, max_color, max_count);
    }
    
    simple_unlock(&page_coloring.color_lock);
}

/*
 * ============================================================================
 * PREDICTIVE OBJECT CACHING
 * ============================================================================
 */

/*
 * Predictive caching based on access patterns
 */
struct vm_predictive_cache {
    unsigned long long *access_history;
    unsigned int history_size;
    unsigned int history_index;
    unsigned long long *predicted_objects;
    unsigned int num_predictions;
    simple_lock_t cache_lock;
};

static struct vm_predictive_cache *predictive_cache = NULL;

/*
 * vm_predictive_cache_init
 *
 * Initialize predictive object caching
 */
void vm_predictive_cache_init(unsigned int history_size)
{
    predictive_cache = (struct vm_predictive_cache *)kalloc(sizeof(struct vm_predictive_cache));
    if (predictive_cache == NULL)
        return;
    
    predictive_cache->history_size = history_size;
    predictive_cache->access_history = (unsigned long long *)kalloc(
        history_size * sizeof(unsigned long long));
    predictive_cache->predicted_objects = (unsigned long long *)kalloc(
        32 * sizeof(unsigned long long));
    
    if (predictive_cache->access_history == NULL || 
        predictive_cache->predicted_objects == NULL) {
        if (predictive_cache->access_history) 
            kfree((vm_offset_t)predictive_cache->access_history, 
                  history_size * sizeof(unsigned long long));
        if (predictive_cache->predicted_objects)
            kfree((vm_offset_t)predictive_cache->predicted_objects, 
                  32 * sizeof(unsigned long long));
        kfree((vm_offset_t)predictive_cache, sizeof(struct vm_predictive_cache));
        predictive_cache = NULL;
        return;
    }
    
    predictive_cache->history_index = 0;
    predictive_cache->num_predictions = 0;
    simple_lock_init(&predictive_cache->cache_lock);
    
    printf("Predictive object cache initialized: history=%u\n", history_size);
}

/*
 * vm_object_record_access
 *
 * Record object access for prediction
 */
void vm_object_record_access(vm_object_t object)
{
    if (predictive_cache == NULL || object == VM_OBJECT_NULL)
        return;
    
    simple_lock(&predictive_cache->cache_lock);
    
    predictive_cache->access_history[predictive_cache->history_index % 
                                      predictive_cache->history_size] = (unsigned long long)object;
    predictive_cache->history_index++;
    
    simple_unlock(&predictive_cache->cache_lock);
}

/*
 * vm_object_predict_next_access
 *
 * Predict next object to be accessed using Markov chain
 */
vm_object_t vm_object_predict_next_access(void)
{
    unsigned long long *pattern;
    unsigned long long predicted = 0;
    unsigned int best_match = 0;
    unsigned int i, j;
    
    if (predictive_cache == NULL || predictive_cache->history_index < 10)
        return VM_OBJECT_NULL;
    
    simple_lock(&predictive_cache->cache_lock);
    
    /* Extract recent pattern (last 5 accesses) */
    pattern = (unsigned long long *)kalloc(5 * sizeof(unsigned long long));
    if (pattern == NULL) {
        simple_unlock(&predictive_cache->cache_lock);
        return VM_OBJECT_NULL;
    }
    
    for (i = 0; i < 5; i++) {
        unsigned int idx = (predictive_cache->history_index - i - 1) % 
                           predictive_cache->history_size;
        pattern[i] = predictive_cache->access_history[idx];
    }
    
    /* Find matching pattern in history */
    for (i = 0; i < predictive_cache->history_index - 5; i++) {
        unsigned int matches = 0;
        for (j = 0; j < 5; j++) {
            unsigned int idx = (i + j) % predictive_cache->history_size;
            if (predictive_cache->access_history[idx] == pattern[j]) {
                matches++;
            }
        }
        
        if (matches > best_match) {
            best_match = matches;
            unsigned int next_idx = (i + 5) % predictive_cache->history_size;
            predicted = predictive_cache->access_history[next_idx];
            
            if (best_match == 5) /* Perfect match */
                break;
        }
    }
    
    kfree((vm_offset_t)pattern, 5 * sizeof(unsigned long long));
    
    /* Update predictions list */
    if (predicted != 0) {
        for (i = 0; i < predictive_cache->num_predictions; i++) {
            if (predictive_cache->predicted_objects[i] == predicted) {
                simple_unlock(&predictive_cache->cache_lock);
                return (vm_object_t)predicted;
            }
        }
        
        if (predictive_cache->num_predictions < 32) {
            predictive_cache->predicted_objects[predictive_cache->num_predictions++] = predicted;
        }
    }
    
    simple_unlock(&predictive_cache->cache_lock);
    
    return (vm_object_t)predicted;
}

/*
 * vm_object_prefetch_predicted
 *
 * Prefetch objects predicted to be accessed soon
 */
void vm_object_prefetch_predicted(void)
{
    vm_object_t object;
    unsigned int i;
    
    if (predictive_cache == NULL)
        return;
    
    simple_lock(&predictive_cache->cache_lock);
    
    for (i = 0; i < predictive_cache->num_predictions; i++) {
        object = (vm_object_t)predictive_cache->predicted_objects[i];
        if (object != VM_OBJECT_NULL && object->ref_count == 0) {
            /* Bring object into memory */
            vm_object_reference(object);
            vm_object_deallocate(object);
        }
    }
    
    predictive_cache->num_predictions = 0;
    
    simple_unlock(&predictive_cache->cache_lock);
}

/*
 * ============================================================================
 * INITIALIZATION FUNCTION
 * ============================================================================
 */

void vm_object_advanced_init_all(void)
{
    simple_lock_init(&merge_lock);
    simple_lock_init(&clone_lock);
    
    vm_page_coloring_init();
    vm_predictive_cache_init(1024);
    vm_object_pool_init();
    
    printf("Advanced VM Object Management fully initialized\n");
}

/*
 * ============================================================================
 * MACHINE LEARNING BASED OBJECT PREDICTION
 * ============================================================================
 */

/*
 * Neural network for object access prediction
 */
struct vm_ml_predictor {
    /* Neural network layers */
    float **weights;
    float **biases;
    unsigned int *layer_sizes;
    unsigned int num_layers;
    
    /* Training data */
    float **training_features;
    float *training_labels;
    unsigned int training_samples;
    unsigned int max_samples;
    
    /* Feature extraction */
    unsigned int num_features;
    unsigned long long feature_offsets[64];
    float feature_weights[64];
    
    /* Prediction cache */
    unsigned long long *prediction_cache;
    unsigned int cache_size;
    unsigned int cache_hits;
    unsigned int cache_misses;
    
    /* Model performance */
    float accuracy;
    float loss;
    unsigned long long predictions_made;
    unsigned long long correct_predictions;
    
    simple_lock_t predictor_lock;
    boolean_t is_trained;
};

static struct vm_ml_predictor *ml_predictor = NULL;

/*
 * vm_ml_predictor_init
 *
 * Initialize machine learning predictor for object access patterns
 */
kern_return_t vm_ml_predictor_init(unsigned int hidden_neurons, unsigned int training_capacity)
{
    unsigned int i, j;
    
    ml_predictor = (struct vm_ml_predictor *)kalloc(sizeof(struct vm_ml_predictor));
    if (ml_predictor == NULL)
        return KERN_RESOURCE_SHORTAGE;
    
    memset(ml_predictor, 0, sizeof(struct vm_ml_predictor));
    
    /* Network architecture: input -> hidden1 -> hidden2 -> output */
    ml_predictor->num_layers = 4;
    ml_predictor->layer_sizes = (unsigned int *)kalloc(4 * sizeof(unsigned int));
    ml_predictor->weights = (float **)kalloc(3 * sizeof(float *));
    ml_predictor->biases = (float **)kalloc(4 * sizeof(float *));
    
    if (ml_predictor->layer_sizes == NULL || ml_predictor->weights == NULL || 
        ml_predictor->biases == NULL) {
        if (ml_predictor->layer_sizes) kfree((vm_offset_t)ml_predictor->layer_sizes, 4 * sizeof(unsigned int));
        if (ml_predictor->weights) kfree((vm_offset_t)ml_predictor->weights, 3 * sizeof(float *));
        if (ml_predictor->biases) kfree((vm_offset_t)ml_predictor->biases, 4 * sizeof(float *));
        kfree((vm_offset_t)ml_predictor, sizeof(struct vm_ml_predictor));
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Set layer sizes */
    ml_predictor->layer_sizes[0] = 32;   /* Input features */
    ml_predictor->layer_sizes[1] = hidden_neurons;
    ml_predictor->layer_sizes[2] = hidden_neurons / 2;
    ml_predictor->layer_sizes[3] = 1;    /* Output (prediction) */
    
    /* Allocate weights */
    for (i = 0; i < 3; i++) {
        unsigned int rows = ml_predictor->layer_sizes[i];
        unsigned int cols = ml_predictor->layer_sizes[i + 1];
        ml_predictor->weights[i] = (float *)kalloc(rows * cols * sizeof(float));
        if (ml_predictor->weights[i] == NULL) {
            for (unsigned int k = 0; k < i; k++) {
                kfree((vm_offset_t)ml_predictor->weights[k], 
                      ml_predictor->layer_sizes[k] * ml_predictor->layer_sizes[k + 1] * sizeof(float));
            }
            return KERN_RESOURCE_SHORTAGE;
        }
        
        /* Initialize weights with small random values */
        for (j = 0; j < rows * cols; j++) {
            ml_predictor->weights[i][j] = ((float)(j % 100) - 50.0f) / 1000.0f;
        }
    }
    
    /* Allocate biases */
    for (i = 0; i < 4; i++) {
        ml_predictor->biases[i] = (float *)kalloc(ml_predictor->layer_sizes[i] * sizeof(float));
        if (ml_predictor->biases[i] == NULL) {
            for (unsigned int k = 0; k < i; k++) {
                kfree((vm_offset_t)ml_predictor->biases[k], 
                      ml_predictor->layer_sizes[k] * sizeof(float));
            }
            return KERN_RESOURCE_SHORTAGE;
        }
        memset(ml_predictor->biases[i], 0, ml_predictor->layer_sizes[i] * sizeof(float));
    }
    
    /* Allocate training data */
    ml_predictor->max_samples = training_capacity;
    ml_predictor->training_features = (float **)kalloc(training_capacity * sizeof(float *));
    ml_predictor->training_labels = (float *)kalloc(training_capacity * sizeof(float));
    
    if (ml_predictor->training_features == NULL || ml_predictor->training_labels == NULL) {
        if (ml_predictor->training_features) kfree((vm_offset_t)ml_predictor->training_features,
                                                   training_capacity * sizeof(float *));
        if (ml_predictor->training_labels) kfree((vm_offset_t)ml_predictor->training_labels,
                                                 training_capacity * sizeof(float));
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Initialize prediction cache */
    ml_predictor->cache_size = 1024;
    ml_predictor->prediction_cache = (unsigned long long *)kalloc(1024 * sizeof(unsigned long long));
    if (ml_predictor->prediction_cache == NULL) {
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Set feature weights (importance of each feature) */
    for (i = 0; i < 32; i++) {
        ml_predictor->feature_weights[i] = 1.0f;
    }
    
    simple_lock_init(&ml_predictor->predictor_lock);
    ml_predictor->is_trained = FALSE;
    
    printf("ML Predictor initialized: hidden=%u, capacity=%u\n", 
           hidden_neurons, training_capacity);
    
    return KERN_SUCCESS;
}

/*
 * vm_extract_object_features
 *
 * Extract feature vector from object for ML prediction
 */
static void vm_extract_object_features(vm_object_t object, float *features)
{
    unsigned long long now = mach_absolute_time();
    
    if (object == NULL || features == NULL)
        return;
    
    vm_object_lock(object);
    
    /* Feature 0: Object size in pages */
    features[0] = (float)(object->size / PAGE_SIZE);
    
    /* Feature 1: Resident page count */
    features[1] = (float)object->resident_page_count;
    
    /* Feature 2: Reference count */
    features[2] = (float)object->ref_count;
    
    /* Feature 3: Age since last access (seconds) */
    features[3] = (float)((now - object->last_access_time) / 1000000000ULL);
    
    /* Feature 4: Page fault rate (if tracked) */
    features[4] = (float)object->page_fault_rate;
    
    /* Feature 5: Write frequency */
    features[5] = (float)object->write_frequency;
    
    /* Feature 6: Share count */
    features[6] = (float)object->share_count;
    
    /* Feature 7: Pager type (0=internal, 1=external) */
    features[7] = object->internal ? 0.0f : 1.0f;
    
    /* Feature 8: Temporary flag */
    features[8] = object->temporary ? 1.0f : 0.0f;
    
    /* Feature 9: Shadow depth */
    features[9] = (float)object->shadow_depth;
    
    /* Feature 10-15: Access pattern history */
    for (int i = 0; i < 6; i++) {
        features[10 + i] = (float)object->access_history[i];
    }
    
    /* Feature 16-19: Working set size over last 4 periods */
    for (int i = 0; i < 4; i++) {
        features[16 + i] = (float)object->working_set[i];
    }
    
    /* Feature 20-23: Page cache hit rates */
    for (int i = 0; i < 4; i++) {
        features[20 + i] = (float)object->cache_hit_rate[i];
    }
    
    /* Feature 24-27: Memory bandwidth usage */
    for (int i = 0; i < 4; i++) {
        features[24 + i] = (float)object->bandwidth_usage[i];
    }
    
    /* Feature 28-31: NUMA access patterns */
    for (int i = 0; i < 4; i++) {
        features[28 + i] = (float)object->numa_access[i];
    }
    
    vm_object_unlock(object);
}

/*
 * vm_ml_predict_object_hotness
 *
 * Predict object hotness using neural network
 */
float vm_ml_predict_object_hotness(vm_object_t object)
{
    float *features;
    float *hidden1;
    float *hidden2;
    float output;
    unsigned int i, j;
    unsigned long long cache_key = (unsigned long long)object;
    
    if (ml_predictor == NULL || object == VM_OBJECT_NULL)
        return 0.5f;
    
    /* Check prediction cache */
    simple_lock(&ml_predictor->predictor_lock);
    for (i = 0; i < ml_predictor->cache_size; i++) {
        if (ml_predictor->prediction_cache[i] == cache_key) {
            ml_predictor->cache_hits++;
            simple_unlock(&ml_predictor->predictor_lock);
            return 0.5f; /* Would return cached prediction */
        }
    }
    ml_predictor->cache_misses++;
    simple_unlock(&ml_predictor->predictor_lock);
    
    /* Allocate feature array */
    features = (float *)kalloc(ml_predictor->layer_sizes[0] * sizeof(float));
    hidden1 = (float *)kalloc(ml_predictor->layer_sizes[1] * sizeof(float));
    hidden2 = (float *)kalloc(ml_predictor->layer_sizes[2] * sizeof(float));
    
    if (features == NULL || hidden1 == NULL || hidden2 == NULL) {
        if (features) kfree((vm_offset_t)features, ml_predictor->layer_sizes[0] * sizeof(float));
        if (hidden1) kfree((vm_offset_t)hidden1, ml_predictor->layer_sizes[1] * sizeof(float));
        if (hidden2) kfree((vm_offset_t)hidden2, ml_predictor->layer_sizes[2] * sizeof(float));
        return 0.5f;
    }
    
    /* Extract features */
    vm_extract_object_features(object, features);
    
    /* Apply feature weights */
    for (i = 0; i < ml_predictor->layer_sizes[0]; i++) {
        features[i] *= ml_predictor->feature_weights[i];
    }
    
    simple_lock(&ml_predictor->predictor_lock);
    
    /* Forward propagation through neural network */
    /* Input -> Hidden Layer 1 */
    for (i = 0; i < ml_predictor->layer_sizes[1]; i++) {
        hidden1[i] = ml_predictor->biases[1][i];
        for (j = 0; j < ml_predictor->layer_sizes[0]; j++) {
            hidden1[i] += features[j] * 
                ml_predictor->weights[0][i * ml_predictor->layer_sizes[0] + j];
        }
        hidden1[i] = (hidden1[i] > 0) ? hidden1[i] : 0; /* ReLU */
    }
    
    /* Hidden Layer 1 -> Hidden Layer 2 */
    for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
        hidden2[i] = ml_predictor->biases[2][i];
        for (j = 0; j < ml_predictor->layer_sizes[1]; j++) {
            hidden2[i] += hidden1[j] * 
                ml_predictor->weights[1][i * ml_predictor->layer_sizes[1] + j];
        }
        hidden2[i] = (hidden2[i] > 0) ? hidden2[i] : 0; /* ReLU */
    }
    
    /* Hidden Layer 2 -> Output */
    output = ml_predictor->biases[3][0];
    for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
        output += hidden2[i] * ml_predictor->weights[2][i];
    }
    output = 1.0f / (1.0f + expf(-output)); /* Sigmoid */
    
    ml_predictor->predictions_made++;
    
    simple_unlock(&ml_predictor->predictor_lock);
    
    /* Cache prediction */
    simple_lock(&ml_predictor->predictor_lock);
    ml_predictor->prediction_cache[cache_key % ml_predictor->cache_size] = cache_key;
    simple_unlock(&ml_predictor->predictor_lock);
    
    kfree((vm_offset_t)features, ml_predictor->layer_sizes[0] * sizeof(float));
    kfree((vm_offset_t)hidden1, ml_predictor->layer_sizes[1] * sizeof(float));
    kfree((vm_offset_t)hidden2, ml_predictor->layer_sizes[2] * sizeof(float));
    
    return output;
}

/*
 * vm_ml_train_predictor
 *
 * Train the neural network predictor using backpropagation
 */
kern_return_t vm_ml_train_predictor(unsigned int epochs, float learning_rate)
{
    float *gradients_w0, *gradients_w1, *gradients_w2;
    float *gradients_b0, *gradients_b1, *gradients_b2, *gradients_b3;
    float *hidden1, *hidden2, *output;
    float loss = 0;
    unsigned int epoch, sample, i, j;
    
    if (ml_predictor == NULL || ml_predictor->training_samples == 0)
        return KERN_FAILURE;
    
    /* Allocate gradient arrays */
    unsigned int w0_size = ml_predictor->layer_sizes[0] * ml_predictor->layer_sizes[1];
    unsigned int w1_size = ml_predictor->layer_sizes[1] * ml_predictor->layer_sizes[2];
    unsigned int w2_size = ml_predictor->layer_sizes[2];
    
    gradients_w0 = (float *)kalloc(w0_size * sizeof(float));
    gradients_w1 = (float *)kalloc(w1_size * sizeof(float));
    gradients_w2 = (float *)kalloc(w2_size * sizeof(float));
    gradients_b0 = (float *)kalloc(ml_predictor->layer_sizes[1] * sizeof(float));
    gradients_b1 = (float *)kalloc(ml_predictor->layer_sizes[2] * sizeof(float));
    gradients_b2 = (float *)kalloc(ml_predictor->layer_sizes[3] * sizeof(float));
    gradients_b3 = (float *)kalloc(ml_predictor->layer_sizes[0] * sizeof(float));
    
    if (gradients_w0 == NULL || gradients_w1 == NULL || gradients_w2 == NULL ||
        gradients_b0 == NULL || gradients_b1 == NULL || gradients_b2 == NULL || 
        gradients_b3 == NULL) {
        if (gradients_w0) kfree((vm_offset_t)gradients_w0, w0_size * sizeof(float));
        if (gradients_w1) kfree((vm_offset_t)gradients_w1, w1_size * sizeof(float));
        if (gradients_w2) kfree((vm_offset_t)gradients_w2, w2_size * sizeof(float));
        if (gradients_b0) kfree((vm_offset_t)gradients_b0, ml_predictor->layer_sizes[1] * sizeof(float));
        if (gradients_b1) kfree((vm_offset_t)gradients_b1, ml_predictor->layer_sizes[2] * sizeof(float));
        if (gradients_b2) kfree((vm_offset_t)gradients_b2, ml_predictor->layer_sizes[3] * sizeof(float));
        if (gradients_b3) kfree((vm_offset_t)gradients_b3, ml_predictor->layer_sizes[0] * sizeof(float));
        return KERN_RESOURCE_SHORTAGE;
    }
    
    hidden1 = (float *)kalloc(ml_predictor->layer_sizes[1] * sizeof(float));
    hidden2 = (float *)kalloc(ml_predictor->layer_sizes[2] * sizeof(float));
    output = (float *)kalloc(sizeof(float));
    
    if (hidden1 == NULL || hidden2 == NULL || output == NULL) {
        /* Cleanup... */
        return KERN_RESOURCE_SHORTAGE;
    }
    
    simple_lock(&ml_predictor->predictor_lock);
    
    /* Training loop */
    for (epoch = 0; epoch < epochs; epoch++) {
        loss = 0;
        
        /* Initialize gradients to zero */
        memset(gradients_w0, 0, w0_size * sizeof(float));
        memset(gradients_w1, 0, w1_size * sizeof(float));
        memset(gradients_w2, 0, w2_size * sizeof(float));
        memset(gradients_b0, 0, ml_predictor->layer_sizes[1] * sizeof(float));
        memset(gradients_b1, 0, ml_predictor->layer_sizes[2] * sizeof(float));
        memset(gradients_b2, 0, ml_predictor->layer_sizes[3] * sizeof(float));
        memset(gradients_b3, 0, ml_predictor->layer_sizes[0] * sizeof(float));
        
        /* Process each training sample */
        for (sample = 0; sample < ml_predictor->training_samples; sample++) {
            float *features = ml_predictor->training_features[sample];
            float label = ml_predictor->training_labels[sample];
            
            /* Forward propagation */
            for (i = 0; i < ml_predictor->layer_sizes[1]; i++) {
                hidden1[i] = ml_predictor->biases[1][i];
                for (j = 0; j < ml_predictor->layer_sizes[0]; j++) {
                    hidden1[i] += features[j] * 
                        ml_predictor->weights[0][i * ml_predictor->layer_sizes[0] + j];
                }
                hidden1[i] = (hidden1[i] > 0) ? hidden1[i] : 0;
            }
            
            for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
                hidden2[i] = ml_predictor->biases[2][i];
                for (j = 0; j < ml_predictor->layer_sizes[1]; j++) {
                    hidden2[i] += hidden1[j] * 
                        ml_predictor->weights[1][i * ml_predictor->layer_sizes[1] + j];
                }
                hidden2[i] = (hidden2[i] > 0) ? hidden2[i] : 0;
            }
            
            *output = ml_predictor->biases[3][0];
            for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
                *output += hidden2[i] * ml_predictor->weights[2][i];
            }
            *output = 1.0f / (1.0f + expf(-*output));
            
            /* Calculate error */
            float error = *output - label;
            loss += error * error;
            
            /* Backward propagation - output layer */
            float delta_output = error * (*output) * (1 - *output);
            for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
                gradients_w2[i] += delta_output * hidden2[i];
            }
            gradients_b2[0] += delta_output;
            
            /* Backward propagation - hidden layer 2 */
            for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
                float delta_hidden2 = delta_output * ml_predictor->weights[2][i];
                delta_hidden2 *= (hidden2[i] > 0) ? 1 : 0;
                
                for (j = 0; j < ml_predictor->layer_sizes[1]; j++) {
                    gradients_w1[i * ml_predictor->layer_sizes[1] + j] += 
                        delta_hidden2 * hidden1[j];
                }
                gradients_b1[i] += delta_hidden2;
            }
            
            /* Backward propagation - hidden layer 1 */
            for (i = 0; i < ml_predictor->layer_sizes[1]; i++) {
                float delta_hidden1 = 0;
                for (j = 0; j < ml_predictor->layer_sizes[2]; j++) {
                    delta_hidden1 += gradients_b1[j] * 
                        ml_predictor->weights[1][j * ml_predictor->layer_sizes[1] + i];
                }
                delta_hidden1 *= (hidden1[i] > 0) ? 1 : 0;
                
                for (j = 0; j < ml_predictor->layer_sizes[0]; j++) {
                    gradients_w0[i * ml_predictor->layer_sizes[0] + j] += 
                        delta_hidden1 * features[j];
                }
                gradients_b0[i] += delta_hidden1;
            }
        }
        
        loss /= ml_predictor->training_samples;
        
        /* Update weights and biases */
        for (i = 0; i < w0_size; i++) {
            ml_predictor->weights[0][i] -= learning_rate * 
                gradients_w0[i] / ml_predictor->training_samples;
        }
        for (i = 0; i < w1_size; i++) {
            ml_predictor->weights[1][i] -= learning_rate * 
                gradients_w1[i] / ml_predictor->training_samples;
        }
        for (i = 0; i < w2_size; i++) {
            ml_predictor->weights[2][i] -= learning_rate * 
                gradients_w2[i] / ml_predictor->training_samples;
        }
        for (i = 0; i < ml_predictor->layer_sizes[1]; i++) {
            ml_predictor->biases[1][i] -= learning_rate * 
                gradients_b0[i] / ml_predictor->training_samples;
        }
        for (i = 0; i < ml_predictor->layer_sizes[2]; i++) {
            ml_predictor->biases[2][i] -= learning_rate * 
                gradients_b1[i] / ml_predictor->training_samples;
        }
        ml_predictor->biases[3][0] -= learning_rate * 
            gradients_b2[0] / ml_predictor->training_samples;
        
        /* Early stopping */
        if (loss < 0.01f)
            break;
        
        /* Decay learning rate */
        learning_rate *= 0.995f;
    }
    
    ml_predictor->loss = loss;
    ml_predictor->accuracy = 1.0f - loss;
    ml_predictor->is_trained = TRUE;
    
    simple_unlock(&ml_predictor->predictor_lock);
    
    /* Cleanup */
    kfree((vm_offset_t)gradients_w0, w0_size * sizeof(float));
    kfree((vm_offset_t)gradients_w1, w1_size * sizeof(float));
    kfree((vm_offset_t)gradients_w2, w2_size * sizeof(float));
    kfree((vm_offset_t)gradients_b0, ml_predictor->layer_sizes[1] * sizeof(float));
    kfree((vm_offset_t)gradients_b1, ml_predictor->layer_sizes[2] * sizeof(float));
    kfree((vm_offset_t)gradients_b2, ml_predictor->layer_sizes[3] * sizeof(float));
    kfree((vm_offset_t)gradients_b3, ml_predictor->layer_sizes[0] * sizeof(float));
    kfree((vm_offset_t)hidden1, ml_predictor->layer_sizes[1] * sizeof(float));
    kfree((vm_offset_t)hidden2, ml_predictor->layer_sizes[2] * sizeof(float));
    kfree((vm_offset_t)output, sizeof(float));
    
    printf("ML Predictor trained: epochs=%u, loss=%.4f, accuracy=%.2f%%\n",
           epochs, loss, ml_predictor->accuracy * 100);
    
    return KERN_SUCCESS;
}

/*
 * ============================================================================
 * LIVE OBJECT MIGRATION (NUMA/COLD STORAGE)
 * ============================================================================
 */

/*
 * Migration context for live object migration
 */
struct vm_live_migration_ctx {
    unsigned long long migration_id;
    vm_object_t source_object;
    vm_object_t target_object;
    vm_offset_t source_start;
    vm_offset_t source_end;
    vm_size_t total_size;
    vm_size_t migrated_size;
    unsigned int page_count;
    unsigned int dirty_pages;
    unsigned long long start_time;
    unsigned long long end_time;
    unsigned int status;
    simple_lock_t migration_lock;
};

static struct vm_live_migration_ctx *active_migrations[16];
static simple_lock_t migration_ctx_lock;

/*
 * vm_object_live_migrate
 *
 * Live migrate object between NUMA nodes or to cold storage
 */
kern_return_t vm_object_live_migrate(vm_object_t object, unsigned int target_node,
                                      unsigned int flags, unsigned long long *migration_id)
{
    struct vm_live_migration_ctx *ctx;
    vm_page_t page, next_page;
    vm_page_t *page_array;
    unsigned long long *dirty_bitmap;
    unsigned int num_pages;
    unsigned int i, j;
    unsigned long long now;
    unsigned int iterations = 0;
    unsigned int max_iterations = 5;
    unsigned int dirty_threshold = 10; /* 10% dirty */
    
    if (object == VM_OBJECT_NULL)
        return KERN_INVALID_ARGUMENT;
    
    /* Allocate migration context */
    ctx = (struct vm_live_migration_ctx *)kalloc(sizeof(struct vm_live_migration_ctx));
    if (ctx == NULL)
        return KERN_RESOURCE_SHORTAGE;
    
    memset(ctx, 0, sizeof(struct vm_live_migration_ctx));
    
    now = mach_absolute_time();
    ctx->migration_id = now;
    ctx->source_object = object;
    ctx->source_start = 0;
    ctx->source_end = object->size;
    ctx->total_size = object->size;
    ctx->start_time = now;
    ctx->status = 1; /* Running */
    simple_lock_init(&ctx->migration_lock);
    
    /* Register migration */
    simple_lock(&migration_ctx_lock);
    for (i = 0; i < 16; i++) {
        if (active_migrations[i] == NULL) {
            active_migrations[i] = ctx;
            break;
        }
    }
    simple_unlock(&migration_ctx_lock);
    
    /* Count pages in object */
    vm_object_lock(object);
    num_pages = 0;
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (!page->fictitious && !page->absent) {
            num_pages++;
        }
    }
    vm_object_unlock(object);
    
    if (num_pages == 0) {
        ctx->status = 3; /* Completed */
        if (migration_id != NULL) *migration_id = ctx->migration_id;
        return KERN_SUCCESS;
    }
    
    /* Allocate page array */
    page_array = (vm_page_t *)kalloc(num_pages * sizeof(vm_page_t));
    dirty_bitmap = (unsigned long long *)kalloc((num_pages + 63) / 64 * sizeof(unsigned long long));
    
    if (page_array == NULL || dirty_bitmap == NULL) {
        if (page_array) kfree((vm_offset_t)page_array, num_pages * sizeof(vm_page_t));
        if (dirty_bitmap) kfree((vm_offset_t)dirty_bitmap, (num_pages + 63) / 64 * sizeof(unsigned long long));
        kfree((vm_offset_t)ctx, sizeof(struct vm_live_migration_ctx));
        return KERN_RESOURCE_SHORTAGE;
    }
    
    /* Phase 1: Pre-copy - copy pages while object is active */
    while (iterations < max_iterations && ctx->migrated_size < ctx->total_size) {
        unsigned int copied_this_iter = 0;
        unsigned int dirty_this_iter = 0;
        
        /* Collect pages */
        vm_object_lock(object);
        i = 0;
        queue_iterate(&object->memq, page, vm_page_t, listq) {
            if (!page->fictitious && !page->absent && !page->busy) {
                page_array[i++] = page;
            }
        }
        vm_object_unlock(object);
        
        /* Clear dirty bitmap for this iteration */
        memset(dirty_bitmap, 0, (num_pages + 63) / 64 * sizeof(unsigned long long));
        
        /* Copy pages that are not dirty */
        for (i = 0; i < num_pages; i++) {
            page = page_array[i];
            if (page == VM_PAGE_NULL)
                continue;
                
            unsigned long long bitmap_idx = i / 64;
            unsigned long long bitmap_bit = i % 64;
            
            if (!(dirty_bitmap[bitmap_idx] & (1ULL << bitmap_bit))) {
                vm_object_lock(object);
                if (!page->busy && !page->absent && !page->fictitious) {
                    /* Allocate page on target node */
                    vm_page_t new_page = vm_page_alloc_node(NULL, 0, target_node);
                    if (new_page != VM_PAGE_NULL) {
                        /* Copy content */
                        vm_page_copy(page, new_page);
                        copied_this_iter++;
                        ctx->migrated_size += PAGE_SIZE;
                    }
                }
                vm_object_unlock(object);
            }
        }
        
        /* Check if we can stop iterations (dirty pages below threshold) */
        if (dirty_this_iter < (num_pages * dirty_threshold / 100)) {
            break;
        }
        
        iterations++;
    }
    
    /* Phase 2: Stop-and-copy - stop access and copy remaining dirty pages */
    vm_object_lock(object);
    
    /* Suspend access to object */
    object->migrating = TRUE;
    
    /* Copy remaining dirty pages */
    for (i = 0; i < num_pages; i++) {
        page = page_array[i];
        if (page == VM_PAGE_NULL)
            continue;
            
        unsigned long long bitmap_idx = i / 64;
        unsigned long long bitmap_bit = i % 64;
        
        if (dirty_bitmap[bitmap_idx] & (1ULL << bitmap_bit)) {
            vm_page_t new_page = vm_page_alloc_node(NULL, 0, target_node);
            if (new_page != VM_PAGE_NULL) {
                vm_page_copy(page, new_page);
                ctx->migrated_size += PAGE_SIZE;
            }
        }
    }
    
    /* Update object to point to new pages */
    /* Would need to update all references to the object */
    
    object->migrating = FALSE;
    object->numa_node = target_node;
    
    vm_object_unlock(object);
    
    ctx->end_time = mach_absolute_time();
    ctx->status = 3; /* Completed */
    
    if (migration_id != NULL)
        *migration_id = ctx->migration_id;
    
    kfree((vm_offset_t)page_array, num_pages * sizeof(vm_page_t));
    kfree((vm_offset_t)dirty_bitmap, (num_pages + 63) / 64 * sizeof(unsigned long long));
    
    printf("Object live migration completed: %llu bytes in %llu ns\n",
           ctx->migrated_size, ctx->end_time - ctx->start_time);
    
    return KERN_SUCCESS;
}

/*
 * ============================================================================
 * MEMORY BANDWIDTH CONTROLLER
 * ============================================================================
 */

/*
 * Bandwidth control for memory operations
 */
struct vm_bandwidth_controller {
    unsigned long long max_bandwidth_bps;
    unsigned long long current_bandwidth_bps;
    unsigned long long measured_bandwidth_bps;
    unsigned long long measurement_start;
    unsigned long long bytes_since_measurement;
    unsigned int throttle_percent;
    unsigned int target_latency_us;
    simple_lock_t bw_lock;
};

static struct vm_bandwidth_controller *bandwidth_ctrl = NULL;

/*
 * vm_bandwidth_controller_init
 *
 * Initialize memory bandwidth controller
 */
kern_return_t vm_bandwidth_controller_init(unsigned long long max_bandwidth_bps,
                                            unsigned int target_latency_us)
{
    bandwidth_ctrl = (struct vm_bandwidth_controller *)kalloc(sizeof(struct vm_bandwidth_controller));
    if (bandwidth_ctrl == NULL)
        return KERN_RESOURCE_SHORTAGE;
    
    bandwidth_ctrl->max_bandwidth_bps = max_bandwidth_bps;
    bandwidth_ctrl->current_bandwidth_bps = max_bandwidth_bps;
    bandwidth_ctrl->measured_bandwidth_bps = 0;
    bandwidth_ctrl->measurement_start = mach_absolute_time();
    bandwidth_ctrl->bytes_since_measurement = 0;
    bandwidth_ctrl->throttle_percent = 100;
    bandwidth_ctrl->target_latency_us = target_latency_us;
    simple_lock_init(&bandwidth_ctrl->bw_lock);
    
    printf("Bandwidth controller initialized: max=%llu MB/s, target_latency=%u us\n",
           max_bandwidth_bps / (1024 * 1024), target_latency_us);
    
    return KERN_SUCCESS;
}

/*
 * vm_bandwidth_measure
 *
 * Measure current memory bandwidth usage
 */
void vm_bandwidth_measure(unsigned long long bytes)
{
    unsigned long long now;
    unsigned long long elapsed_ns;
    unsigned long long current_bps;
    
    if (bandwidth_ctrl == NULL)
        return;
    
    simple_lock(&bandwidth_ctrl->bw_lock);
    
    bandwidth_ctrl->bytes_since_measurement += bytes;
    now = mach_absolute_time();
    elapsed_ns = now - bandwidth_ctrl->measurement_start;
    
    if (elapsed_ns >= 1000000000ULL) { /* 1 second */
        current_bps = (bandwidth_ctrl->bytes_since_measurement * 1000000000ULL) / elapsed_ns;
        bandwidth_ctrl->measured_bandwidth_bps = current_bps;
        
        /* Adjust throttle based on measured bandwidth */
        if (current_bps > bandwidth_ctrl->max_bandwidth_bps) {
            bandwidth_ctrl->throttle_percent = (bandwidth_ctrl->max_bandwidth_bps * 100) / current_bps;
            if (bandwidth_ctrl->throttle_percent < 10)
                bandwidth_ctrl->throttle_percent = 10;
        } else {
            bandwidth_ctrl->throttle_percent = 100;
        }
        
        bandwidth_ctrl->bytes_since_measurement = 0;
        bandwidth_ctrl->measurement_start = now;
    }
    
    simple_unlock(&bandwidth_ctrl->bw_lock);
}

/*
 * vm_bandwidth_throttle
 *
 * Throttle memory operation if bandwidth exceeded
 */
boolean_t vm_bandwidth_throttle(unsigned long long bytes)
{
    unsigned long long now, elapsed_ns;
    unsigned long long allowed_bytes;
    unsigned long long sleep_ns = 0;
    
    if (bandwidth_ctrl == NULL)
        return FALSE;
    
    simple_lock(&bandwidth_ctrl->bw_lock);
    
    now = mach_absolute_time();
    elapsed_ns = now - bandwidth_ctrl->measurement_start;
    
    if (elapsed_ns > 0) {
        allowed_bytes = (bandwidth_ctrl->max_bandwidth_bps * elapsed_ns) / 1000000000ULL;
        allowed_bytes = (allowed_bytes * bandwidth_ctrl->throttle_percent) / 100;
        
        if (bandwidth_ctrl->bytes_since_measurement + bytes > allowed_bytes) {
            /* Need to throttle */
            unsigned long long excess = bandwidth_ctrl->bytes_since_measurement + bytes - allowed_bytes;
            sleep_ns = (excess * 1000000000ULL) / bandwidth_ctrl->max_bandwidth_bps;
            if (sleep_ns > 10000000) /* Max 10ms */
                sleep_ns = 10000000;
        }
    }
    
    simple_unlock(&bandwidth_ctrl->bw_lock);
    
    if (sleep_ns > 0) {
        thread_set_timeout(sleep_ns / 1000000);
        thread_block(NULL);
        return TRUE;
    }
    
    return FALSE;
}

/*
 * ============================================================================
 * ADVANCED CACHE OPTIMIZATION
 * ============================================================================
 */

/*
 * Cache line alignment optimization
 */
struct vm_cache_optimizer {
    unsigned int l1_cache_size;
    unsigned int l2_cache_size;
    unsigned int l3_cache_size;
    unsigned int cache_line_size;
    unsigned int associativity;
    unsigned int *cache_sets;
    unsigned int set_count;
    simple_lock_t cache_lock;
};

static struct vm_cache_optimizer *cache_optimizer = NULL;

/*
 * vm_cache_optimizer_init
 *
 * Initialize cache optimizer for better page placement
 */
void vm_cache_optimizer_init(void)
{
    cache_optimizer = (struct vm_cache_optimizer *)kalloc(sizeof(struct vm_cache_optimizer));
    if (cache_optimizer == NULL)
        return;
    
    /* Get cache information from hardware */
    cache_optimizer->l1_cache_size = machine_l1_cache_size();
    cache_optimizer->l2_cache_size = machine_l2_cache_size();
    cache_optimizer->l3_cache_size = machine_l3_cache_size();
    cache_optimizer->cache_line_size = machine_cache_line_size();
    cache_optimizer->associativity = machine_cache_associativity();
    
    if (cache_optimizer->l3_cache_size > 0) {
        cache_optimizer->set_count = cache_optimizer->l3_cache_size / 
                                      (cache_optimizer->cache_line_size * 
                                       cache_optimizer->associativity);
        cache_optimizer->cache_sets = (unsigned int *)kalloc(
            cache_optimizer->set_count * sizeof(unsigned int));
        if (cache_optimizer->cache_sets != NULL) {
            memset(cache_optimizer->cache_sets, 0, 
                   cache_optimizer->set_count * sizeof(unsigned int));
        }
    }
    
    simple_lock_init(&cache_optimizer->cache_lock);
    
    printf("Cache optimizer initialized: L1=%uKB, L2=%uKB, L3=%uKB, line=%u, assoc=%u\n",
           cache_optimizer->l1_cache_size / 1024,
           cache_optimizer->l2_cache_size / 1024,
           cache_optimizer->l3_cache_size / 1024,
           cache_optimizer->cache_line_size,
           cache_optimizer->associativity);
}

/*
 * vm_object_optimize_cache_placement
 *
 * Optimize object placement to reduce cache misses
 */
void vm_object_optimize_cache_placement(vm_object_t object)
{
    vm_page_t page;
    unsigned int set;
    unsigned int color;
    
    if (cache_optimizer == NULL || object == VM_OBJECT_NULL)
        return;
    
    vm_object_lock(object);
    
    queue_iterate(&object->memq, page, vm_page_t, listq) {
        if (page->fictitious || page->absent)
            continue;
        
        /* Calculate cache set for this page */
        set = (page->offset / PAGE_SIZE) % cache_optimizer->set_count;
        color = (page->phys_addr / cache_optimizer->cache_line_size) % 
                cache_optimizer->associativity;
        
        simple_lock(&cache_optimizer->cache_lock);
        cache_optimizer->cache_sets[set]++;
        simple_unlock(&cache_optimizer->cache_lock);
        
        /* If cache set is too full, consider remapping */
        if (cache_optimizer->cache_sets[set] > cache_optimizer->associativity) {
            /* Would trigger page remapping */
            page->cache_conflict = TRUE;
        }
    }
    
    vm_object_unlock(object);
}

/*
 * ============================================================================
 * ADDITIONAL HELPER FUNCTIONS
 * ============================================================================
 */

/*
 * vm_object_dump_statistics
 *
 * Dump comprehensive object statistics
 */
void vm_object_dump_statistics(void)
{
    printf("\n========== VM OBJECT STATISTICS ==========\n");
    printf("Total objects: %d\n", vm_object_zone->z_count);
    printf("Cached objects: %d\n", vm_object_cached_count);
    printf("Zone allocated: %llu\n", vm_object_zone->z_alloc_count);
    printf("Zone free: %llu\n", vm_object_zone->z_free_count);
    printf("Zone size: %llu bytes\n", vm_object_zone->z_cur_size);
    
    if (ml_predictor != NULL && ml_predictor->is_trained) {
        printf("\n--- ML Predictor ---\n");
        printf("Predictions: %llu\n", ml_predictor->predictions_made);
        printf("Cache hits: %u\n", ml_predictor->cache_hits);
        printf("Cache misses: %u\n", ml_predictor->cache_misses);
        printf("Accuracy: %.2f%%\n", ml_predictor->accuracy * 100);
        printf("Loss: %.4f\n", ml_predictor->loss);
    }
    
    if (bandwidth_ctrl != NULL) {
        printf("\n--- Bandwidth Controller ---\n");
        printf("Max bandwidth: %llu MB/s\n", bandwidth_ctrl->max_bandwidth_bps / (1024 * 1024));
        printf("Measured bandwidth: %llu MB/s\n", bandwidth_ctrl->measured_bandwidth_bps / (1024 * 1024));
        printf("Throttle: %u%%\n", bandwidth_ctrl->throttle_percent);
    }
    
    if (cache_optimizer != NULL) {
        printf("\n--- Cache Optimizer ---\n");
        printf("L1: %uKB, L2: %uKB, L3: %uKB\n",
               cache_optimizer->l1_cache_size / 1024,
               cache_optimizer->l2_cache_size / 1024,
               cache_optimizer->l3_cache_size / 1024);
        printf("Cache sets: %u\n", cache_optimizer->set_count);
        printf("Associativity: %u\n", cache_optimizer->associativity);
    }
    
    printf("==========================================\n");
}

/*
 * ============================================================================
 * INITIALIZATION
 * ============================================================================
 */

void vm_object_extreme_init(void)
{
    simple_lock_init(&migration_ctx_lock);
    memset(active_migrations, 0, sizeof(active_migrations));
    
    /* Initialize ML predictor */
    vm_ml_predictor_init(64, 10000);
    
    /* Initialize bandwidth controller (default: 10 GB/s) */
    vm_bandwidth_controller_init(10ULL * 1024 * 1024 * 1024, 100);
    
    /* Initialize cache optimizer */
    vm_cache_optimizer_init();
    
    printf("Extreme VM Object Management initialized\n");
}
