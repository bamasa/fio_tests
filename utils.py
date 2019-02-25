import json
import numpy as np
from copy import deepcopy
from collections import OrderedDict


class NestedDictEncoder():
    """Encode/decode nested dicts."""

    def __init__(self, features_types_dict, all_nums=True):
        '''
        Args:
            features_types_dict (dict of OrderedDict): initial nested dict with features types as values,
                where "cat" -- categorical and "num" -- numerical features.
            all_nums (bool): whether all values are numerical.

        TODO:
            VAE and/or other encoders

        '''
        self.features_types_dict = OrderedDict(features_types_dict)

        self._features_types = list(
            self._get_nested_dict_values(self.features_types_dict))
        if all_nums:
            self._features_types = ['num' for _ in self._features_types]
        else:
            for f in self._features_types:
                if f not in ['num', 'cat']:
                    raise ValueError('Values must be "num" or "cat"')
        self._num_features_idx = [i for i, t in enumerate(
            self._features_types) if t == 'num']
        self._cat_features_idx = [i for i, t in enumerate(
            self._features_types) if t == 'cat']
        self._cat_val_to_code = {}
        self._code_to_cat_val = {}
        self._clean_temp()

    def encode(self, nested_dict):
        """Encodes nested dict."""
        nested_dict = self.order_dict(nested_dict)
        values = list(self._get_nested_dict_values(nested_dict))
        values = self._encode_categorical_features(values)
        return values

    def decode(self, values):
        """Decodes nested dict."""
        temp_dict = self.features_types_dict.copy()
        self._temp_idx = 0
        self._temp_values = list(values).copy()
        self._temp_values = self._decode_categorical_features(
            self._temp_values)
        self._set_nested_dict_values(temp_dict)
        decoded_dict = temp_dict
        self._clean_temp()
        return decoded_dict

    def order_dict(self, nested_dict):
        """Orders dict by initial dict."""
        temp_dict = self.features_types_dict.copy()
        self._temp_values = list(self._get_nested_dict_values_by_order(
            nested_dict, self.features_types_dict))
        self._temp_idx = 0
        self._set_nested_dict_values(temp_dict)
        ordered_dict = temp_dict
        self._clean_temp()
        return ordered_dict

    def keys_list(self):
        return list(self._get_nested_dict_keys(self.features_types_dict))

    def _clean_temp(self):
        self._temp_idx = None
        self._temp_values = None

    def _get_nested_dict_values(self, d):
        for v in d.values():
            if isinstance(v, dict):
                yield from self._get_nested_dict_values(v)
            else:
                yield v

    def _get_nested_dict_keys(self, d):
        for k, v in d.items():
            if isinstance(v, dict):
                yield from self._get_nested_dict_keys(v)
            else:
                yield k

    def _set_nested_dict_values(self, d):
        for k, v in d.items():
            if isinstance(v, dict):
                self._set_nested_dict_values(d[k])
            else:
                d[k] = self._temp_values[self._temp_idx]
                self._temp_idx += 1

    def _get_nested_dict_values_by_order(self, d, ordered_d):
        for k in ordered_d:
            v = d[k]
            if isinstance(v, dict):
                v_ordered = ordered_d[k]
                yield from self._get_nested_dict_values_by_order(v, v_ordered)
            else:
                yield v

    def _encode_categorical_features(self, values):
        encoded_values = values.copy()
        for i in self._cat_features_idx:
            v = values[i]
            if v not in self._cat_val_to_code:
                code = len(self._cat_val_to_code)
                self._cat_val_to_code[v] = code
                self._code_to_cat_val[code] = v
            encoded_values[i] = self._cat_val_to_code[v]
        return encoded_values

    def _decode_categorical_features(self, values, default='-999'):
        decoded_values = values.copy()
        for i in self._cat_features_idx:
            v = values[i]
            cat = self._code_to_cat_val[
                v] if v in self._code_to_cat_val else default
            decoded_values[i] = cat
        return decoded_values
