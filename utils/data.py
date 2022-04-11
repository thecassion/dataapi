def is_dict_simple(data_in):
    """
    Returns True if data_in is a simple dictionary.
    """
    for key, value in data_in.items():
        if isinstance(value, dict) :
            return False
    return True

def repeat_question(data,format_out,questions, dataInToDataOut):
    """
    Returns a list of data_in repeated questions times.
    """
    data_out = []
    for question in questions:
        if question["code"] in data and "uid" in question:
            __data_out = {
                "uid": question["uid"],
                "response": data[question["code"]],
            }
            data_out.append(dataInToDataOut(__data_out, {}, format_out,questions))
    return data_out
def dataInToDataOut(data_in, format_in, format_out, questions)->dict:
    """
    Converts data_in to data_out.
    """
    if ((format_in is None) or (format_in=={})) and (is_dict_simple(data_in)):
        try:
            if isinstance(format_out,str):
                if format_out in data_in:
                    data_out = data_in[format_out]
                    return data_out
                pass
            elif isinstance(format_out,dict):
                data_out = {}
                if len(format_out)>0:
                    for key, value in format_out.items():
                        data_out[key] = dataInToDataOut(data_in, format_in, value,questions)
                        print(data_out)
                    return data_out
                for key, value in data_in.items():
                    data_out[key] = value
                    return data_out
            elif isinstance(format_out,list):
                data_out = []
                if len(format_out)==1 or len(format_out)>2 or ("repeat" not in format_out[0]):
                    for value in format_out:
                        data_out.append(dataInToDataOut(data_in, format_in, value,questions))
                    return data_out
                elif len(format_out)==2 and "repeat" in format_out[0]:
                    data_out = repeat_question(data_in,format_out[1],questions,dataInToDataOut)
                    return data_out
                #     else:
                #         for key, value in format_out.items():
                #             data_out[key] = dataInToDataOut(data_in, format_in, value)
                #         return data_out
                else:
                    return None
            else:
                return None
        except Exception as e:
            raise ValueError("format_out is not valid", e)