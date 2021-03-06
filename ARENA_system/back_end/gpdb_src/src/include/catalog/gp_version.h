#ifndef _GP_VERSION_H_
#define _GP_VERSION_H_

#include "catalog/genbki.h"
/*
 * Defines for gp_version table
 */
#define GpVersionRelationName		"gp_version_at_initdb"

#define GpVersionRelationId 5003

CATALOG(gp_version_at_initdb,5003) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	int16		schemaversion;
	text		productversion;
} FormData_gp_version;

/* no foreign key */

#define Natts_gp_version				2
#define Anum_gp_version_schemaversion		1
#define Anum_gp_version_productversion		2

DATA(insert ( 2  "6.0.0-beta.1+995f61cc build dev" ));

#endif /*_GP_VERSION_H_*/
