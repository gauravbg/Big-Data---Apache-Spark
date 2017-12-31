from tifffile import TiffFile
import io
import numpy as np
import zipfile
from numpy.linalg import svd
from pyspark import SparkConf, SparkContext
import hashlib
import random
from scipy.spatial import distance

# Change this method to slice the 2d array
def cutter(arr, r, c):
    subs = np.array_split(arr, c, axis=1)
    all = []
    for sub in subs:
        all.append(np.array_split(sub, r, axis=0))
    full = []
    for _ in zip(*all):
        full.extend(_)
    return full

def getOrthoTif(zfBytes, zipfn):

    zipfn = zipfn.encode('utf-8')
    #given a zipfile as bytes (i.e. from reading from a binary file),
    #return a np array of rgbx values for each pixel
    bytesio = io.BytesIO(zfBytes)
    zfiles = zipfile.ZipFile(bytesio, "r")
    print_sub_images = ["3677454_2025195.zip-0", "3677454_2025195.zip-1", "3677454_2025195.zip-18", "3677454_2025195.zip-19"]

    #find tif:
    for fn in zfiles.namelist():
        if fn[-4:] == '.tif':#found it, turn into array:
            tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))
            all_subs = cutter(tif.asarray(), 5, 5)
            result = []
            for i, sub_image in enumerate(all_subs):
                sub_fn = "{}-{}".format(zipfn, i)
                result.append((sub_fn, sub_image))
                if sub_fn in print_sub_images:
                    print("First Pixel {} :".format(sub_fn) , sub_image[0][0])
            return result

def convertRGBToIntensity(fn, image):

    intensity = lambda x: int((sum(x[0:3])/3.0) * (x[3]/100.0))
    intensity_image = np.apply_along_axis(intensity, axis=2, arr=image)
    all_pieces = cutter(intensity_image, 50, 50)
    scaled_values = np.asarray([np.mean(piece) for piece in all_pieces])
    return fn, scaled_values.reshape(50, 50)

def scaleValues(num):
    if num <-1:
        return -1
    elif num>1:
        return 1
    else:
        return 1

def featureVectors(fn, image):
    row_diff = np.diff(image, axis=1).flatten()
    row_diff = np.where(row_diff > 1, 1,
             (np.where(row_diff < -1, -1, 0)))
    # row_diff = map(lambda x: scaleValues(x), row_diff)
    col_diff = np.diff(image, axis=0).flatten()
    col_diff = np.where(col_diff > 1, 1,
                        (np.where(col_diff < -1, -1, 0)))
    # col_diff = map(lambda x: scaleValues(x), col_diff)
    print_feature_vector = ["3677454_2025195.zip-1", "3677454_2025195.zip-18"]
    if fn in print_feature_vector:
        print("Feature Vector {} :".format(fn), np.concatenate((row_diff, col_diff)))
    return fn, np.concatenate((row_diff, col_diff))

def featureDigest(fn, feature):
    # key = [str(x) for x in feature]
    start = 0
    size = 38
    end = start+size
    chunks_count = 0
    digest = []
    while chunks_count < 128:
        md5 = hashlib.md5(feature[start:end])
        # md5.update("".join(key[start:end]))
        int_digest = int.from_bytes(md5.digest(), byteorder='little') % 4 #Tune the mod value to form more or less candidate pairs
        digest.append(str(int_digest))
        chunks_count+=1
        start+=size
        if chunks_count == 92:
            size+=1
        end+=size
    return fn, "".join(digest)

def splitBands(fn, digest):
    start = 0
    size = end = 4
    bandNum = 0
    result = []
    while end <= (len(digest)+1):
        key = str(bandNum) + '-' + digest[start:end]
        result.append((key, fn))
        start += size
        end += size
        bandNum += 1
    return result

def reform_list(key, files):
    required = ["3677454_2025195.zip-0", "3677454_2025195.zip-1", "3677454_2025195.zip-18", "3677454_2025195.zip-19"]
    result = []
    for file in required:
        if file in files:
            candidates = [x for x in files if x != file]
            result.append((file, candidates))
    if len(result) == 0:
        result.append((required[0], []))
    return result


def reform_set(key, files):
    required = {"3677454_2025195.zip-0", "3677454_2025195.zip-1", "3677454_2025195.zip-18", "3677454_2025195.zip-19"}
    result = []
    for file in required:
        if file in files:
            result.append((file, files-{file}))
    if len(result) == 0:
        result.append((list(required)[0], set()))
    return result

def indexRecords(fn, feature):
    index = random.randint(0, 20) #Tune this for number of PCA calculation chunks
    return index, (fn, feature)

def performPCA(index, records):
    result = []
    record_count = len(records)
    features_count = 4900
    full = []
    for fn, feature in records:
        full.extend(feature)
    mat = np.asarray(full).reshape(record_count, features_count)
    cov = np.cov(mat, rowvar=False)
    U, S, V = svd(cov)
    U_reduce_T = U[:,:10].transpose()
    for fn, feature in records:
        f = np.asarray(feature).reshape(4900, 1)
        z = np.dot(U_reduce_T, f)
        result.append((fn, z))
    return result

def mapWithCandidates(fn, feature):
    result = []
    for req, candidates in candidates_map.value:
        if fn in candidates:
            result.append((req, (fn, feature)))
        elif fn == req:
            result.append((req, ("SELF_FILE", feature)))
    if len(result) == 0:
        result.append(("DUMMY", (fn, feature)))
    return result

def calculateDistances(key_file, candidates):
    key_vector = None
    candidate_map = dict()
    for fn, vector in candidates:
        if fn == "SELF_FILE":
            key_vector = vector
        else:
            candidate_map[fn] = vector
    result = []
    for fn, vector in candidate_map.items():
        result.append((fn, distance.euclidean(key_vector, vector)))
    result.sort(key=lambda x: x[1])
    return key_file, result


def to_set(a):
    return {a}

def use_set(a):
    return a

def add(a, b):
    a.add(b)
    return a

def union(a, b):
    a.union(b)
    return a

def to_list(a):
    return [a]

def use_list(a):
    return a

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a


if __name__ == "__main__":

   # Configure Spark
   # APP_NAME = "A2_GAURAV_GOPALKRISHNA"
   # conf = SparkConf().setAppName(APP_NAME)
   # conf = conf.setMaster("local[*]")
   sc   = SparkContext()

   # rdd = sc.binaryFiles('/home/gauravbg/SBU-fall-17/Big_Data/Assignment2/a2_small_sample')
   # rdd = sc.binaryFiles('hdfs:/data/large_sample')
   rdd = sc.binaryFiles('hdfs:/data/small_sample')

    #PART - 1
   rdd = rdd.flatMap(lambda x:getOrthoTif(x[1], x[0].split('/')[-1]))

   # PART - 2
   rdd = rdd.map(lambda x: convertRGBToIntensity(*x))
   features_rdd = rdd.map(lambda x: featureVectors(*x))

   # PART - 3A
   digest_rdd = features_rdd.map(lambda x: featureDigest(*x))
   digest_bands_rdd = digest_rdd.flatMap(lambda x: splitBands(*x))
   grouped_rdd = digest_bands_rdd.combineByKey(to_set, add, union)
   filtered_rdd = grouped_rdd.flatMap(lambda x: reform_set(*x))
   candidates_rdd = filtered_rdd.combineByKey(use_set, union, union)

   # PART - 3B
   candidates_map = sc.broadcast(candidates_rdd.collect())
   indexed_features_rdd = features_rdd.map(lambda x: indexRecords(*x))
   split_features_rdd = indexed_features_rdd.combineByKey(to_list, append, extend)
   reduced_dimen_rdd = split_features_rdd.flatMap(lambda x: performPCA(*x))
   required_reduced_dimen_rdd = reduced_dimen_rdd.flatMap(lambda x: mapWithCandidates(*x))
   required_reduced_dimen_rdd = required_reduced_dimen_rdd.filter(lambda x: x[0] != "DUMMY")
   candidates_reduced_dimend_rdd = required_reduced_dimen_rdd.combineByKey(to_list, append, extend)
   sorted_distances_rdd = candidates_reduced_dimend_rdd.map(lambda x: calculateDistances(*x))

   final_out = sorted_distances_rdd.collect()
   for i in range(len(final_out)):
       feature = final_out[i]
       print("Filename:", feature[0], feature[1])

