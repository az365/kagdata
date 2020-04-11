import csv


def split_csv_row(line, delimiter=None):
    for row in csv.reader([line], delimiter) if delimiter else csv.reader([line]):
        return row


def apply_dict_to_field(record, field, dict_to_apply, default=None):
    value = record.get(field)
    record[field] = dict_to_apply.get(value, default or value)
    return record


def add_fields(record, additional_fields):
    record.update(additional_fields)
    return record


def crop_cells(row, max_len=33, substitute='...', crop_str_only=True):
    result = list()
    subst_len = len(substitute)
    limit_len = max_len - subst_len
    assert limit_len > 0
    for cell in row:
        output_cell = cell
        if isinstance(cell, str) or not crop_str_only:
            str_cell = str(cell)
            if len(str_cell) > max_len:
                output_cell = str_cell[:limit_len] + substitute
        result.append(output_cell)
    return result


def union_duplicate_fields(record, list_duplicate_fields=(('a1', 'a2'), ('b1', 'b2', 'b3'))):
    for duplicate_fields_group in list_duplicate_fields:
        main_field = duplicate_fields_group[0]
        first_value = None, None
        for field in duplicate_fields_group:
            cur_value = record.get(field)
            if cur_value is not None:
                first_value = cur_value
        if first_value:
            record[main_field] = first_value
            for field in duplicate_fields_group[1:]:
                record.pop(field, None)
    return record


def transpose_records_list(records_list):
    record_out = dict()
    for r in records_list:
        for k, v in r.items():
            record_out[k] = record_out.get(k, []) + [v]
    return record_out


def get_histograms(records, fields=tuple(), max_values=25, ignore_none=False):
    histograms = dict()
    for r in records:
        for f in fields or r.keys():
            if f not in histograms:
                histograms[f] = dict()
            cur_hist = histograms[f]
            cur_value = r.get(f)
            cur_count = cur_hist.get(cur_value, 0)
            can_add_new_key = len(cur_hist) < max_values
            if (cur_count or can_add_new_key) and (cur_value is not None or not ignore_none):
                cur_hist[cur_value] = cur_count + 1
    for k, v in histograms.items():
        yield k, v

