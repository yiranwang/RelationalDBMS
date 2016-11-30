#include "qe.h"

bool opCompare(void* ref1, void* ref2, CompOp op, AttrType type) {
    if (op == NO_OP) {
        return true;
    }
    if (!ref1 || !ref2) {
        return false;
    }
    if (type == TypeVarChar) {
        //printf("comparing %s and %s\n", (char*)ref1, (char*)ref2);
        string str1((char*)ref1);
        string str2((char*)ref2);
        int res = str1.compare(str2);
        switch (op) {
            case EQ_OP: return res == 0;
            case LT_OP: return res < 0;
            case LE_OP: return res <= 0;
            case GT_OP: return res > 0;
            case GE_OP: return res >= 0;
            case NE_OP: return res != 0;
            default: return false;
        }
    }
    else if (type == TypeInt) {
        int int1 = *(int*)ref1;
        int int2 = *(int*)ref2;
        int res = int1 - int2;
        //printf("comparing %d and %d\n", int1, int2);
        switch (op) {
            case EQ_OP: return res == 0;
            case LT_OP: return res < 0;
            case LE_OP: return res <= 0;
            case GT_OP: return res > 0;
            case GE_OP: return res >= 0;
            case NE_OP: return res != 0;
            default: return false;
        }

    }
    else {
        float f1 = *(float*)ref1;
        float f2 = *(float*)ref2;
        float res = f1 - f2;
        //printf("comparing %f and %f\n", f1, f2);
        switch (op) {
            case EQ_OP: return res == 0.0;
            case LT_OP: return res < 0.0;
            case LE_OP: return res <= 0.0;
            case GT_OP: return res > 0.0;
            case GE_OP: return res >= 0.0;
            case NE_OP: return res != 0.0;
            default: return false;
        }
    }
    return false;
}


struct KeyAndValue {
    string key;
    AttrType type;
    float keyFloat;
};

KeyAndValue getKeyAndValue(const void *data, string attribute, vector<Attribute> attrs, string &key) {
    KeyAndValue result;
    int targetFieldNum = -1;

    // find the attribute field number count
    for (int i = 0; i < attrs.size(); i++) {
        if (strcmp(attribute.c_str(), attrs[i].name.c_str()) == 0) {
            targetFieldNum = i;
            break;
        }
    }

    char *temp = (char*)data;

    int isNull = temp[(int)(ceil((targetFieldNum + 1.0) / 8.0)) - 1] & (1 << (7 - (targetFieldNum % 8)));

    if (isNull == 1) {
        key = "1";
        return result;
    }

    short offset = ceil((double)attrs.size() / 8.0); // nullIndicator

    for (int i = 0; i < targetFieldNum; i++) {
        isNull = temp[(int)(ceil((i + 1.0) / 8.0)) - 1] & (1 << (7 - (i % 8)));

        if (isNull != 0) {
            continue;
        }else {

            if (attrs[i].type == TypeVarChar) {
                int len = *(int*)((char*)data + offset);
                offset += len + sizeof(int);
            }else {
                offset += 4;
            }
        }
    }

    key = "0";

    if (attrs[targetFieldNum].type == TypeVarChar) {
        int len = *(int*)((char*)data + offset);

        void *keyTemp = malloc(len + 1);
        memcpy((char*)keyTemp, (char*)data + sizeof(int), len);
        *((char*)keyTemp + len) = '\0';

        string s((char*)keyTemp);
        key += s;

        free(keyTemp);
        result.type = TypeVarChar;
        result.keyFloat = 0.0;
        result.key = key;

    }else if (attrs[targetFieldNum].type == TypeInt) {
        int value = *(int*)((char*)data + offset);

        stringstream ss;
        ss << value;
        key += ss.str();

        result.key = key;
        result.type = TypeInt;
        result.keyFloat = value;

    }else {
        float value = *(float*)((char*)data + offset);

        stringstream ss;
        ss << value;
        key += ss.str();

        result.key = key;
        result.type = TypeReal;
        result.keyFloat = value;
    }

    return result;
}

unsigned calculateBytes(const vector<Attribute> &recordDescriptor, const void *data) {
    int fieldNum = recordDescriptor.size();
    int nullIndCount = ceil((double)fieldNum / 8.0);

    char* temp = (char*)data;

    unsigned offset = nullIndCount; // null indicator size

    for (int i = 0; i < fieldNum; i++) {

        int isNull = temp[(int)(ceil((i + 1.0) / 8.0)) - 1] & (1 << (7 - (i % 8)));

        if (isNull == 0) {
            if (recordDescriptor[i].type == TypeVarChar) {
                int len = *(int*)((char*)data + offset);

                offset += sizeof(int) + len;
            }else {
                offset += sizeof(int);
            }
        }
    }

    return offset;
}