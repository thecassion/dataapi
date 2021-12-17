
def dataInToDataOut(data_in, format_in, format_out)->dict:
    """
    Converts data_in to data_out.
    """
    if ((format_in is None) or (format_in=={})) and (is_dict_simple(data_in)):
        data_out=format_out.copy()
        for key, value in format_out.items():
            if isinstance(value, dict) :
                if len(value):
                    data_out[key]=dataInToDataOut(data_in, {}, value)
                else:
                    for k,v in data_in.items():
                        data_out[key][k]=v
            elif isinstance(value, list):
                data_out[key]=[]
                if len(value):
                    for item in value:
                        data_out[key].append(dataInToDataOut(data_in, {}, item))
                else:
                    for k1,v1 in data_in.items():
                        data_out[key].append({k1:v1})
            else:
                data_out[key]= data_in[value]
        return data_out


def is_dict_simple(data_in):
    """
    Returns True if data_in is a simple dictionary.
    """
    for key, value in data_in.items():
        if isinstance(value, dict) :
            return False
    return True