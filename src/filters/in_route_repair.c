	/*
 *			GPAC - Multimedia Framework C SDK
 *
 *			Authors: Jean Le Feuvre
 *			Copyright (c) Telecom ParisTech 2018-2024
 *					All rights reserved
 *
 *  This file is part of GPAC / ROUTE (ATSC3, DVB-I) input filter
 *
 *  GPAC is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation; either version 2, or (at your option)
 *  any later version.
 *
 *  GPAC is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; see the file COPYING.  If not, write to
 *  the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#include "in_route.h"

#ifndef GPAC_DISABLE_ROUTE



#include <gpac/isomedia.h>

//simple dependency extraction from isobmff:
//if error returns GF_FALSE, otherwise GF_TRUE
//if all samples are random access point, ranges is set to NULL and nb_ranges set to 0
//otherwise ranges is allocated with the dependency list
//the function is very basic and will need further rework, listng all direct reference samples
static Bool routein_repair_get_isobmf_deps(const char *seg_name, GF_Blob *blob, SampleRangeDependency **out_ranges, u32 *nb_ranges)
{
	GF_ISOFile *file;
	u64 BytesMissing;
	u32 i, count;
	SampleRangeDependency *ranges;
	char szBlobPath[100];
	if ((!blob && !seg_name) || !out_ranges || !nb_ranges) return GF_FALSE;
	*out_ranges = NULL;
	*nb_ranges = 0;
	GF_Err e = gf_isom_open_progressive_ex("isobmff://", 0, 0, 0, &file, &BytesMissing, NULL);
	if (e) return GF_FALSE;
	if (blob) {
		sprintf(szBlobPath, "gmem://%p", blob);
		e = gf_isom_open_segment(file, szBlobPath, 0, 0, 0);
	} else {
		e = gf_isom_open_segment(file, seg_name, 0, 0, 0);
	}
	if (e) {
		gf_isom_delete(file);
		return GF_FALSE;
	}
	s32 max_cts_o = (s32) gf_isom_get_max_sample_cts_offset(file, 1);
	s32 min_cts_o = gf_isom_get_min_negative_cts_offset(file, 1, GF_ISOM_MIN_NEGCTTS_ANY);

	count = gf_isom_get_sample_count(file, 1);
	if (!max_cts_o || !count) {
		gf_isom_delete(file);
		return GF_TRUE;
	}

	ranges = gf_malloc(sizeof(SampleRangeDependency)*count);
	memset(ranges, 0, sizeof(SampleRangeDependency)*count);
	*out_ranges = ranges;
	*nb_ranges = count;

	GF_ISOSample static_sample;
	s32 ctso_last_sap = -1;
	s32 max_ctso = -1;
	u32 cur_id_sap = 0;
	u32 cur_id_p = 0;
	u32 nb_levels=0;
	for (i=0; i<count; i++) {
		SampleRangeDependency *r = &ranges[i];
		u64 offset;
		s32 cts_offset;
		memset(&static_sample, 0, sizeof(static_sample));
		GF_ISOSample *samp = gf_isom_get_sample_info_ex(file, 1, i+1, NULL, &offset, &static_sample);
		if (!samp) break;
		r->size = samp->dataLength;
		r->offset = (u32) offset;
		cts_offset = samp->CTS_Offset;
		//translate to unsigned cts offset
		if (min_cts_o<0) cts_offset -= cts_offset;

		if (samp->IsRAP) {
			ctso_last_sap = cts_offset;
			max_ctso = -1;
			cur_id_sap += cur_id_p + nb_levels+1;
			r->id = cur_id_sap;
			r->dep_id = cur_id_sap;
			cur_id_p = cur_id_sap;
			nb_levels = 0;
			r->type = 1;
			continue;
		}
		if ((max_ctso<0) || (cts_offset==max_ctso)) {
			max_ctso = cts_offset;
			cur_id_p += nb_levels+1;
			nb_levels = 1;
			r->id = cur_id_p;
			r->dep_id = cur_id_sap;
			continue;
		}
		if (cts_offset) {
			nb_levels = 2;
			r->id = cur_id_p+1;
			r->dep_id = cur_id_p;
			continue;
		}
		if (nb_levels == 2) nb_levels = 3;
		else if (nb_levels==1) nb_levels = 2;
		r->id = cur_id_p + nb_levels-1;
		r->dep_id = cur_id_p + nb_levels-2;
		r->type = 2;
	}
	gf_isom_delete(file);

#ifndef GPAC_DISABLE_LOG
	if (gf_log_tool_level_on(GF_LOG_ROUTE, GF_LOG_DEBUG)) {
		GF_LOG(GF_LOG_DEBUG, GF_LOG_ROUTE, ("[ROUTE] Range dependency for file %s\n", seg_name ? seg_name : szBlobPath));
		for (i=0; i<count; i++) {
			SampleRangeDependency *r = &ranges[i];
			GF_LOG(GF_LOG_DEBUG, GF_LOG_ROUTE, ("\t#%3d size %10u offset %10u ID %2u depends on range ID %2u\n", i+1, r->size, r->offset, r->id, r->dep_id));
		}
	}
#endif
	return GF_TRUE;
}


//patch TS file, replacing all 188 bytes packets overlaping a gap by padding packets
static Bool routein_repair_segment_ts_local(ROUTEInCtx *ctx, GF_ROUTEEventFileInfo *finfo)
{
    u32 i, pos;
    Bool drop_if_first = GF_FALSE;
    u8 *data = finfo->blob->data;


    pos = 0;
    for (i=0; i<finfo->nb_frags; i++) {
        u32 start_range = finfo->frags[i].offset;
        u32 end_range = finfo->frags[i].size;

		//if we missed first 4 packets, we cannot rely on PAT/PMT being present in the rest of the segment
		//we could further check this at the demux level, but for now we drop the segment
        if (!i && (start_range>4*188))
			drop_if_first = GF_TRUE;

        end_range += start_range;
        //reset all missed byte ranges as padding packets
        start_range -= pos;
        while (start_range % 188) start_range++;
        while (pos<start_range) {
            data[pos] = 0x47;
            data[pos+1] = 0x1F;
            data[pos+2] = 0xFF;
            data[pos+3] = 0x10;
            pos += 188;
        }
        //end range not aligned with a packet start, rewind position to prev packet start
		while (end_range % 188) end_range--;
		pos = end_range;
    }
    //and patch all end packets
    while (pos<finfo->blob->size) {
        data[pos] = 0x47;
        data[pos+1] = 0x1F;
        data[pos+2] = 0xFF;
        data[pos+3] = 0x10;
        pos += 188;
    }
	//remove corrupted flag
	finfo->blob->flags &= ~GF_BLOB_CORRUPTED;
	return drop_if_first;
}

//top boxes we look for in segments
static const char *top_codes[] = {"styp", "emsg", "prft", "moof", "mdat", "free", "sidx", "ssix"};
static u32 nb_top_codes = GF_ARRAY_LENGTH(top_codes);

static u32 next_top_level_box(GF_ROUTEEventFileInfo *finfo, u8 *data, u32 size, u32 *cur_pos, u32 *box_size)
{
    u32 pos = *cur_pos;
	u32 cur_frag = 0;
	while (cur_frag < finfo->nb_frags) {
		//in range, can go
		if ((finfo->frags[cur_frag].offset <= pos) && (finfo->frags[cur_frag].offset + finfo->frags[cur_frag].size > pos)) {
			break;
		}
		//before range, adjust pos
		if (finfo->frags[cur_frag].offset > pos) {
			pos = finfo->frags[cur_frag].offset;
			break;
		}
		//after range, go to next
		cur_frag++;
		//current pos is outside last valid range, no more top-level boxes to parse
		if (cur_frag==finfo->nb_frags)
			return 0;
	}

    while (pos + 8 < size) {
        u32 i;
        u32 type_idx = 0;
		u32 first_box = 0;
		u32 first_box_size = 0;
        for (i=0; i<nb_top_codes; i++) {
			if ((data[pos]==top_codes[i][0]) && (data[pos+1]==top_codes[i][1]) && (data[pos+2]==top_codes[i][2]) && (data[pos+3]==top_codes[i][3])) {
				first_box = pos;
				type_idx = i;
				break;
            }
        }
        //we need at least 4 bytes size header
        if (first_box<4) {
            pos++;
            continue;
        }
		first_box_size = GF_4CC(data[first_box-4], data[first_box-3], data[first_box-2], data[first_box-1]);
		if (first_box_size<8) {
			pos++;
			continue;
		}
        *cur_pos = first_box-4;
        *box_size = first_box_size;
        return GF_4CC(top_codes[type_idx][0], top_codes[type_idx][1], top_codes[type_idx][2], top_codes[type_idx][3]);
    }
    return 0;
}

//patch ISOBMFF file, replacing all top-level boxes overlaping a gap by free boxes
static void routein_repair_segment_isobmf_local(ROUTEInCtx *ctx, GF_ROUTEEventFileInfo *finfo)
{
    u8 *data = finfo->blob->data;
    u32 size = finfo->blob->size;
    u32 pos = 0;
	u32 prev_moof_pos = 0;
    //walk through all possible top-level boxes in order
    //if box completely in a received byte range, keep as is
    //if mdat or free box, keep as is
    //otherwise change box type to free
    while ((u64)pos + 8 < size) {
        u32 i;
		Bool is_mdat = GF_FALSE;
        Bool box_complete = GF_FALSE;
        u32 prev_pos = pos;
        u32 box_size = 0;
        u32 type = next_top_level_box(finfo, data, size, &pos, &box_size);
        //no more top-level found, patch from current pos until end of payload
        if (!type) {
            u32 remain = size - pos;
            gf_assert(remain);
            if (remain<8) {
                GF_LOG(GF_LOG_ERROR, GF_LOG_ROUTE, ("[ROUTE] Failed to patch end of corrupted segment, segment size not big enough to hold the final box header, something really corrupted in source data\n"));
                return;
            }
            data[pos] = (remain>>24) & 0xFF;
            data[pos+1] = (remain>>16) & 0xFF;
            data[pos+2] = (remain>>8) & 0xFF;
            data[pos+3] = (remain) & 0xFF;
            data[pos+4] = 'f';
            data[pos+5] = 'r';
            data[pos+6] = 'e';
            data[pos+7] = 'e';
			//remove corrupted flag
			finfo->blob->flags &= ~GF_BLOB_CORRUPTED;
            return;
        }
        //we missed a box header, insert one at previous pos, indicating a free box !!
        if (pos > prev_pos) {
            u32 missed_size = pos - prev_pos;
            data[prev_pos] = (missed_size>>24) & 0xFF;
            data[prev_pos+1] = (missed_size>>16) & 0xFF;
            data[prev_pos+2] = (missed_size>>8) & 0xFF;
            data[prev_pos+3] = (missed_size) & 0xFF;
            data[prev_pos+4] = 'f';
            data[prev_pos+5] = 'r';
            data[prev_pos+6] = 'e';
            data[prev_pos+7] = 'e';
        }
        if (type == GF_4CC('f','r','e','e')) {
            box_complete = GF_TRUE;
        } else if (type == GF_4CC('m','d','a','t')) {
			if (ctx->repair != ROUTEIN_REPAIR_STRICT) {
				box_complete = GF_TRUE;
			} else {
				is_mdat = GF_TRUE;
			}
		} else if (type == GF_4CC('m','o','o','f')) {
			prev_moof_pos = pos;
		}

        if (!box_complete) {
            //box is only partially received
            for (i=0; i<finfo->nb_frags; i++) {
                if (pos + box_size < finfo->frags[i].offset)
                    break;
                if ((pos >= finfo->frags[i].offset) && (pos+box_size<=finfo->frags[i].offset + finfo->frags[i].size)) {
                    box_complete = GF_TRUE;
                    break;
                }
            }
        }
        if (box_complete) {
            pos += box_size;
            continue;
        }
		//incomplete mdat (strict mode), discard previous moof
		if (is_mdat) {
			data[prev_moof_pos+4] = 'f';
			data[prev_moof_pos+5] = 'r';
			data[prev_moof_pos+6] = 'e';
			data[prev_moof_pos+7] = 'e';
		}
        //incomplete box, move to free (not changing size)
        data[pos+4] = 'f';
        data[pos+5] = 'r';
        data[pos+6] = 'e';
        data[pos+7] = 'e';
        pos += box_size;
    }
	//remove corrupted flag
	finfo->blob->flags &= ~GF_BLOB_CORRUPTED;
}

static Bool does_belong(GF_LCTFragInfo *frags, u32 nb_frags, u32 start, u32 size) {
	// check if intervale [start, start+size[ belongs to frags list of intervales
	u32 i;
	if(start > GF_UINT_MAX - size) return GF_FALSE;
	for(i=0; i < nb_frags; i++) {
		if(start < frags[i].offset) return GF_FALSE;
		if(start < frags[i].offset + frags[i].size) {
			return start + size <= frags[i].offset + frags[i].size;
		}
	}
	return GF_FALSE;
}

static void route_repair_build_ranges_isobmf(ROUTEInCtx *ctx, RepairSegmentInfo *rsi, GF_ROUTEEventFileInfo *finfo, int position) {
	u32 pos = position;
	u32 min_size = 1024;
	u32 box_size, box_type;

	while(pos+8 <= finfo->total_size) { // if

		if(!does_belong(finfo->frags, finfo->nb_frags, pos, 8)) {
			RouteRepairRange *rr = gf_list_pop_back(ctx->seg_range_reservoir);
			if (!rr) {
				GF_SAFEALLOC(rr, RouteRepairRange);
				if (!rr) {
					rsi->nb_errors++;
					return;
				}
			} else {
				memset(rr, 0, sizeof(RouteRepairRange));
			}

			rr->br_start = pos;
			rr->br_end = MIN(finfo->total_size, pos + min_size);

			GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] adding repair range [%u, %u] \n", rr->br_start, rr->br_end));

			gf_list_add(rsi->ranges, rr);
			rsi->last_pos_repair_top_level = rr->br_start;
			return;
		} else {
			box_size = GF_4CC(finfo->blob->data[pos], finfo->blob->data[pos+1], finfo->blob->data[pos+2], finfo->blob->data[pos+3]);
			box_type = GF_4CC(finfo->blob->data[pos+4], finfo->blob->data[pos+5], finfo->blob->data[pos+6], finfo->blob->data[pos+7]);
			if(pos + box_size > finfo->total_size) {
				GF_LOG(GF_LOG_WARNING, GF_LOG_ROUTE, ("[REPAIR] Corrupted data: top-level box range [%u, %u] exceeds segment size %u \n", pos, pos+box_size, finfo->total_size));
				return;
			}
			if ((box_type != GF_4CC('m', 'd', 'a', 't')) && (box_type != GF_4CC('i', 'd', 'a', 't'))) {
				if(!does_belong(finfo->frags, finfo->nb_frags, pos, box_size)) {
					RouteRepairRange *rr = gf_list_pop_back(ctx->seg_range_reservoir);
					if (!rr) {
						GF_SAFEALLOC(rr, RouteRepairRange);
						if (!rr) {
							rsi->nb_errors++;
							// check next box
							pos += box_size;
							continue;
						}
					} else {
						memset(rr, 0, sizeof(RouteRepairRange));
					}

					rr->br_start = pos;
					rr->br_end = MIN(finfo->total_size, MAX(pos + box_size, pos + min_size));

					GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] adding repair range [%u, %u] \n", rr->br_start, rr->br_end));

					gf_list_add(rsi->ranges, rr);
					rsi->last_pos_repair_top_level = rr->br_start;
				}
			}
			pos += box_size;
		}
	}
}

static u32 routein_repair_isobmf_frames(ROUTEInCtx *ctx, RepairSegmentInfo *rsi, SampleRangeDependency *r, u32 threshold) {
	// add byte ranges to "rsi->ranges" for repair
	u32 nb_renages = 0;
	u32 i;
	RouteRepairRange *rr = NULL;

	for (i=0; i<=rsi->finfo.nb_frags; i++) {
		u32 br_start = 0, br_end = 0;
		// first range
		if (!i) {
			br_end = rsi->finfo.frags[i].offset;
		}
		//middle ranges
		else if (i < rsi->finfo.nb_frags) {
			br_start = rsi->finfo.frags[i-1].offset + rsi->finfo.frags[i-1].size;
			br_end = rsi->finfo.frags[i].offset;
		}
		//last range
		else {
			br_start = rsi->finfo.frags[rsi->finfo.nb_frags-1].offset + rsi->finfo.frags[rsi->finfo.nb_frags-1].size;
			br_end = UINT32_MAX;
		}

		//this was correctly received !
		if (br_end <= br_start) continue;
		//byte range is before sample range
		if (br_end <= r->offset) continue;

		if (br_end > r->offset && br_start < r->offset+r->size) {

			if(rr && br_start - rr->br_end < threshold) {
				rr->br_end = br_end;
			} else {
				if(rr) {
					nb_renages++;
					gf_list_add(rsi->ranges, rr);
					GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Repair frames: adding range [%u, %u[ for repair\n", rr->br_start, rr->br_end));
				}

				rr = gf_list_pop_back(ctx->seg_range_reservoir);
				if (!rr) {
					GF_SAFEALLOC(rr, RouteRepairRange);
					if (!rr) {
						rsi->nb_errors++;
						continue;
					}
				} else {
					memset(rr, 0, sizeof(RouteRepairRange));
				}

				rr->br_start = MAX(r->offset, br_start);
				rr->br_end = MIN(r->offset + r->size, br_end);
			}
		}
		//byte range is after sample range
		if (br_start >= r->offset + r->size) {
			if(rr) {
				nb_renages++;
				gf_list_add(rsi->ranges, rr);
				GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Repair frames: adding range [%u, %u[ for repair\n", rr->br_start, rr->br_end));
			}
			break;
		}
	}

	return nb_renages;
}

static void route_repair_isobmf_mdat_box(ROUTEInCtx *ctx, RepairSegmentInfo *rsi) {
	u32 nb_ranges, i;
	u32 threshold = 1024;
	
	routein_repair_get_isobmf_deps(rsi->finfo.filename, rsi->finfo.blob, &rsi->srd, &nb_ranges);

	for (i=0; i<nb_ranges; i++) {
		SampleRangeDependency *r = &rsi->srd[i];
		if(r->id == r->dep_id) {
			routein_repair_isobmf_frames(ctx, rsi, r, threshold);

		} else if(r->type == 2) {
			continue;
		}
	}

	gf_free(rsi->srd);
}

static void route_repair_build_ranges_full(ROUTEInCtx *ctx, RepairSegmentInfo *rsi, GF_ROUTEEventFileInfo *finfo)
{
	u32 i;

	//collect decoder stats, or if not found direct output
	if (ctx->opid) {
		GF_FilterPidStatistics stats;
		GF_Err e = gf_filter_pid_get_statistics(ctx->opid, &stats, GF_STATS_DECODER_SINK);
		if (e) e = gf_filter_pid_get_statistics(ctx->opid, &stats, GF_STATS_SINK);
		if (!e) {
			//log is set as warning for now as this is work in progress
			GF_LOG(GF_LOG_WARNING, GF_LOG_ROUTE, ("[RPUTE] Repairing segment - buffer status: %d ms (%u for rebuffer %u max buffer)\n", (u32) (stats.buffer_time/1000) , (u32) (stats.min_playout_time/1000), (u32) (stats.max_buffer_time/1000) ));
		}
	}

	//compute byte range - max ranges to repair: if N interval received, at max N+1 interval losts
	//TODO, select byte range priorities & co, check if we want multiple byte ranges??
	for (i=0; i<=finfo->nb_frags; i++) {
		u32 br_start = 0, br_end = 0;
		// first range
		if (!i) {
			br_end = finfo->frags[i].offset;
		}
		//middle ranges
		else if (i < finfo->nb_frags) {
			br_start = finfo->frags[i-1].offset + rsi->finfo.frags[i-1].size;
			br_end  = finfo->frags[i].offset;
		}
		//last range
		else {
			br_start = finfo->frags[finfo->nb_frags-1].offset + finfo->frags[finfo->nb_frags-1].size;
			br_end = finfo->total_size;
		}

		//this was correctly received !
		if (br_end <= br_start) continue;

		RouteRepairRange *rr = gf_list_pop_back(ctx->seg_range_reservoir);
		if (!rr) {
			GF_SAFEALLOC(rr, RouteRepairRange);
			if (!rr) {
				rsi->nb_errors++;
				continue;
			}
		} else {
			memset(rr, 0, sizeof(RouteRepairRange));
		}
		rr->br_start = br_start;
		rr->br_end = br_end;

		gf_list_add(rsi->ranges, rr);
	}

}

void routein_queue_repair(ROUTEInCtx *ctx, GF_ROUTEEventType evt, u32 evt_param, GF_ROUTEEventFileInfo *finfo)
{
	//TODO handle late data
	RepairSegmentInfo *rsi;
	if (evt==GF_ROUTE_EVT_LATE_DATA) {
		GF_LOG(GF_LOG_ERROR, GF_LOG_ROUTE, ("[ROUTE] Late data patching not yet implemented !\n"));
		return;
	}
	if (ctx->repair==ROUTEIN_REPAIR_NO) {
		routein_on_event_file(ctx, evt, evt_param, finfo, GF_FALSE, GF_FALSE);
		return;
	}
	if (ctx->repair<ROUTEIN_REPAIR_FULL) {
		Bool drop_if_first = GF_FALSE;
		if (finfo->blob->mx)
			gf_mx_p(finfo->blob->mx);

		if (strstr(finfo->filename, ".ts") || strstr(finfo->filename, ".m2ts")) {
			drop_if_first = routein_repair_segment_ts_local(ctx, finfo);
		} else {
			routein_repair_segment_isobmf_local(ctx, finfo);
		}

		if (finfo->blob->mx)
			gf_mx_v(finfo->blob->mx);
		routein_on_event_file(ctx, evt, evt_param, finfo, GF_FALSE, drop_if_first);
		return;
	}
	//TODO, pass any repair URL info coming from broadcast
	if (!ctx->repair_url) {
		routein_on_event_file(ctx, evt, evt_param, finfo, GF_FALSE, GF_FALSE);
		return;
	}

	//queue up our repair
	rsi = gf_list_pop_back(ctx->seg_repair_reservoir);
	if (!rsi) {
		GF_SAFEALLOC(rsi, RepairSegmentInfo);
		rsi->ranges = gf_list_new();
	}

	if (!rsi) {
		GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Failed to allocate repair entry %s (TSI=%u, TOI=%u)\n", finfo->filename, finfo->tsi, finfo->toi));
		routein_on_event_file(ctx, evt, evt_param, finfo, GF_FALSE, GF_FALSE);
		return;
	}
	GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Queing repair for object %s (TSI=%u, TOI=%u)\n", finfo->filename, finfo->tsi, finfo->toi));

	rsi->evt = evt;
	rsi->service_id = evt_param;
	rsi->finfo = *finfo;

	if(ctx->repair == ROUTEIN_REPAIR_FULL) {
		route_repair_build_ranges_full(ctx, rsi, finfo);
	} else if(ctx->repair == ROUTEIN_REPAIR_ISOBMF) {
		rsi->state = 0;
		route_repair_build_ranges_isobmf(ctx, rsi, finfo, 0);

		if(gf_list_count(rsi->ranges) == 0) {
			rsi->state = 1;
			route_repair_isobmf_mdat_box(ctx, rsi);
		}
	} else {
		GF_LOG(GF_LOG_WARNING, GF_LOG_ROUTE, ("[REPAIR] repair option not supported \n", ctx->repair));
	}

	if(gf_list_count(rsi->ranges) == 0) {
		gf_list_transfer(ctx->seg_range_reservoir, rsi->ranges);
		gf_list_add(ctx->seg_range_reservoir, rsi->ranges);
		gf_list_add(ctx->seg_range_reservoir, rsi);
		return;
	}

	gf_mx_p(finfo->blob->mx);
	if (finfo->blob->flags & GF_BLOB_IN_TRANSFER)
		rsi->was_partial = GF_TRUE;
	finfo->blob->flags |= GF_BLOB_IN_TRANSFER;
	gf_mx_v(finfo->blob->mx);

	gf_list_add(ctx->seg_repair_queue, rsi);
}



static void repair_session_done(ROUTEInCtx *ctx, RouteRepairSession *rsess, GF_Err res_code)
{
	RepairSegmentInfo *rsi = rsess->current_si;
	if (!rsi) return;

	//notify routedmx we have received a byte range
	gf_routedmx_patch_frag_info(ctx->route_dmx, rsi->service_id, &rsi->finfo, rsess->range->br_start, rsess->range->br_start + rsess->range->done);
	if(rsess->range->br_end == rsess->range->br_start + rsess->range->done) {
		GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Successfully repaired data interval [%u, %u] \n", rsess->range->br_start, rsess->range->br_end));
	} else {
		GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Failed to repair entire data interval [%u, %u]. Only sub-interval [%u, %u] was received.. \n", rsess->range->br_start, rsess->range->br_end, rsess->range->br_start, rsess->range->br_start+rsess->range->done));
	}

	rsess->current_si = NULL;
	gf_list_add(ctx->seg_range_reservoir, rsess->range);
	rsess->range = NULL;
	rsi->pending--;
	if (res_code<0) rsi->nb_errors++;
	if (rsi->pending) return;
	if (rsi->removed) {
		gf_list_transfer(ctx->seg_range_reservoir, rsi->ranges);
	}

	if (gf_list_count(rsi->ranges)) return;

	if(ctx->repair == ROUTEIN_REPAIR_ISOBMF && !rsi->state) {
		route_repair_build_ranges_isobmf(ctx, rsi, &(rsi->finfo), rsi->last_pos_repair_top_level);
		if (gf_list_count(rsi->ranges)) return;

		rsi->state = 1;
		route_repair_isobmf_mdat_box(ctx, rsi);
	}

	if (!rsi->removed) {
		//flush
		gf_mx_p(rsi->finfo.blob->mx);
		if (!rsi->nb_errors) rsi->finfo.blob->flags &= ~GF_BLOB_CORRUPTED;
		if (!rsi->was_partial) rsi->finfo.blob->flags &= ~GF_BLOB_IN_TRANSFER;
		gf_mx_v(rsi->finfo.blob->mx);

		GF_LOG(GF_LOG_INFO, GF_LOG_ROUTE, ("[REPAIR] Repair done for object %s (TSI=%u, TOI=%u)%s\n", rsi->finfo.filename, rsi->finfo.tsi, rsi->finfo.toi, rsi->nb_errors ? " - errors remain" : ""));

		routein_on_event_file(ctx, rsi->evt, rsi->service_id, &rsi->finfo, GF_TRUE, GF_FALSE);
	}

	gf_list_del_item(ctx->seg_repair_queue, rsi);
	GF_List *bck = rsi->ranges;
	memset(rsi, 0, sizeof(RepairSegmentInfo));
	rsi->ranges = bck;
	gf_list_add(ctx->seg_repair_reservoir, rsi);
}

#define REPAIR_BUF_SIZE	5000
static void repair_session_run(ROUTEInCtx *ctx, RouteRepairSession *rsess)
{
	GF_Err e;
	RepairSegmentInfo *rsi;
	char http_buf[REPAIR_BUF_SIZE];

restart:
	rsi = rsess->current_si;
	if (!rsi) {
		RouteRepairRange *rr = NULL;
		u32 i, count;
		count = gf_list_count(ctx->seg_repair_queue);
		for (i=0; i<count;i++) {
			u32 j, nb_ranges;
			rsi = gf_list_get(ctx->seg_repair_queue, i);
			nb_ranges = gf_list_count(rsi->ranges);
			for (j=0; j<nb_ranges; j++) {
				rr = gf_list_get(rsi->ranges, j);
				//todo check priotiry
			}
			if (rr) break;
			rsi = NULL;
		}
		if (!rsi) return;
		rsess->current_si = rsi;
		rsi->pending++;
		rsess->range = rr;
		gf_list_del_item(rsi->ranges, rr);

		char *url = gf_url_concatenate(ctx->repair_url, rsi->finfo.filename);

		if (!rsess->dld) {
			GF_DownloadManager *dm = gf_filter_get_download_manager(ctx->filter);
			rsess->dld = gf_dm_sess_new(dm, url, GF_NETIO_SESSION_NOT_CACHED | GF_NETIO_SESSION_NOT_THREADED | GF_NETIO_SESSION_PERSISTENT, NULL, NULL, &e);
			if (rsess->dld)
				gf_dm_sess_set_netcap_id(rsess->dld, "__ignore");
		} else {
			e = gf_dm_sess_setup_from_url(rsess->dld, url, GF_FALSE);
		}
		gf_free(url);
		if (e) {
			GF_LOG(GF_LOG_ERROR, GF_LOG_ROUTE, ("[REPAIR] Failed to setup download session for %s: %s\n", rsi->finfo.filename, gf_error_to_string(e)));
			repair_session_done(ctx, rsess, e);
			return;
		}
		gf_dm_sess_set_range(rsess->dld, rsess->range->br_start, rsess->range->br_end-1, GF_TRUE);
	}

	u32 offset = rsess->range->br_start + rsess->range->done;
	u32 nb_read=0;
	e = gf_dm_sess_fetch_data(rsess->dld, http_buf, REPAIR_BUF_SIZE, &nb_read);
	if (e==GF_IP_NETWORK_EMPTY) return;

	if (offset + nb_read > rsess->range->br_end)
		e = GF_REMOTE_SERVICE_ERROR;

	if (nb_read && (e>=GF_OK)) {
		gf_mx_p(rsi->finfo.blob->mx);
		memcpy(rsi->finfo.blob->data + offset, http_buf, nb_read);
		gf_mx_v(rsi->finfo.blob->mx);
		rsess->range->done += nb_read;
		//do we need to update frag info at each chunk ? for now, only done when session is over

	}
	if (e==GF_OK) return;

	repair_session_done(ctx, rsess, e);
	if (e<0) return;

	goto restart;

}

GF_Err routein_do_repair(ROUTEInCtx *ctx)
{
	u32 i, nb_active=0;

	for (i=0; i<ctx->max_sess; i++) {
		RouteRepairSession *rsess = &ctx->http_repair_sessions[i];
		repair_session_run(ctx, rsess);
		if (rsess->current_si) nb_active++;
	}
	return nb_active ? GF_OK : GF_EOS;
}


void routein_repair_mark_file(ROUTEInCtx *ctx, u32 service_id, const char *filename, Bool is_delete)
{
	u32 i, count = gf_list_count(ctx->seg_repair_queue);
	for (i=0; i<count; i++) {
		RepairSegmentInfo *rsi = gf_list_get(ctx->seg_repair_queue, i);
		if ((rsi->service_id==service_id) && !strcmp(rsi->finfo.filename, filename)) {
			//we don't cancel sessions now, this should be done in session_done
			if (is_delete) {
				rsi->removed = GF_TRUE;
			} else {
				//TODO: decide if we need to be more agressive ?
			}
			return;
		}
	}
}


#endif /* GPAC_DISABLE_ROUTE */

