import numpy as np
from scipy.special import psi as psi
from scipy.special import polygamma as pg
number_of_topics = 5


def calculate_new_alpha(init_alpha, gamma_list, max_iter = 1):
    alpha = np.copy(init_alpha)
    M = len(gamma_list)
    K = number_of_topics
    g = np.zeros(K)
    g_sum_gamma = np.zeros(K)
    for i in range (K):
        for d in range (M):
            g_sum_gamma[i] += psi(gamma_list[d][i]) - psi(sum(gamma_list[d]))

    for it in range(max_iter):
        g = M *(psi(sum(alpha)) - psi(alpha)) + g_sum_gamma
        #H = M * (special.polygamma(1, sum(alpha)) - np.diag(special.polygamma(1, alpha)))
        z = M * pg(1, sum(alpha))
        h = -M * pg(1, alpha)
        c = sum(g/h)/ (1/z + sum(np.ones(number_of_topics)/h))
        new_alpha = alpha - ((g - np.ones(K) * c) / h)



        if (new_alpha < 0).sum() > 0:
            init_alpha = np.copy(init_alpha) / 10.0
            return calculate_new_alpha(init_alpha, gamma_list, max_iter)

        diff = np.sum(np.abs(alpha - new_alpha))
        print diff, it
        alpha = new_alpha
        if diff < 1e-5 and it > 1:
            return alpha

    print alpha
    return alpha


def alpha_nr(gamma_matrix, maxit=1, init_alpha=[]):
    M, K = gamma_matrix.shape
    if not len(init_alpha) > 0:
        init_alpha = gamma_matrix.mean(axis=0) / K
    alpha = init_alpha.copy()
    alphap = init_alpha.copy()
    g_term = (psi(gamma_matrix) - psi(gamma_matrix.sum(axis=1))[:, None]).sum(axis=0)
    for it in range(maxit):
        grad = M * (psi(alpha.sum()) - psi(alpha)) + g_term
        H = -M * np.diag(pg(1, alpha)) + M * pg(1, alpha.sum())
        alpha_new = alpha - np.dot(np.linalg.inv(H), grad)
        if (alpha_new < 0).sum() > 0:
            init_alpha /= 10.0
            return alpha_nr(gamma_matrix,maxit=maxit, init_alpha=init_alpha)

        diff = np.sum(np.abs(alpha - alpha_new))
        alpha = alpha_new
        print diff, it
        if diff < 1e-6 and it > 1:
            return alpha
    print alpha
    return alpha

alpha = np.random.uniform(low=0.0, high=1.0, size=5)
gamma_matrix = [[21.668369367446278, 15.064807454680196, 0.69132698249244173, 13.797809303716447, 0.18450343166463368], [10.127001605621414, 1.4492045307233032, 11.667414797343973, 17.082871150203555, 11.080324456107752], [3.2358777186974836, 1.9196201474809946, 5.5192089802622544, 24.165633500408251, 16.566476193151015], [17.896782287704585, 19.230792130153091, 0.77591558355903489, 7.3698441329507141, 6.1334824056325727], [0.04624092302702603, 17.136633393635254, 6.7471011719007494, 10.890007946542468, 16.586833104894499], [0.046688165163085479, 0.05214885433792539, 3.6254958152559191, 28.928846707305777, 18.753636997937289], [21.646354538745857, 1.6390607596835893, 0.73691218764542343, 15.759391092161101, 11.625097961764034], [9.1671237322643684, 0.030912270000150222, 14.15338346539119, 0.56985731056032429, 27.485539761783969], [10.68563902195619, 14.218648160074521, 3.3109929951053654, 5.4642362108152405, 17.727300152048684], [0.048548624870062639, 3.9376438262046847, 10.338561605158727, 16.529328012085188, 20.55273447168134], [20.89862485272532, 18.691039508872475, 1.5778431275539211, 3.2590790518552657, 6.9802299989930212], [9.2536579536925458, 7.3387705279335975, 14.78097264822738, 0.56761779821118719, 19.46579761193529], [0.046230710000306055, 48.28520510853398, 0.69132674000000005, 1.084354295385715, 1.2996996860800023], [2.1726956720485791, 0.030912269999999999, 0.69132674000000005, 37.731414895134591, 10.780466962816826], [0.046232971004752071, 6.1820735187878011, 16.351496533052625, 12.927808790466898, 15.899204726687916], [0.046230710000000001, 11.401630045455144, 0.69132674000000005, 0.59128823607349545, 38.676340808471359], [5.6606695033079912, 14.560591853604508, 2.9588712515173596, 0.49804292999999999, 27.728641001570129], [11.622375775721292, 8.1740547026467336, 0.83773647021655862, 10.241949960224339, 20.53069963119108], [4.8679012797049026, 14.742272562938757, 4.392126135047544, 10.776550335719623, 16.627966226589177], [15.29257961377046, 16.141827556285669, 0.97647170456358889, 1.5513057219184418, 17.444631943461843], [28.559209335295073, 20.974610107604658, 1.1738590811584484, 0.55881737495999428, 0.14032064098183225], [15.418748441937613, 7.8565570800844879, 3.9253799620951164, 10.611501386322409, 13.59462966956038], [14.762976296411276, 8.9639899122558617, 3.9100201387589308, 15.112408087450335, 8.6574221051235938], [4.541607070803205, 0.3220599637073796, 1.4298999558001386, 21.592579112644508, 23.520670437044775], [2.2027893142607735, 13.729131191663702, 5.8374267102954285, 14.965640931754638, 14.671828392025457], [12.508704080886233, 3.249860467300111, 10.227379737804927, 10.041033880073396, 15.379838373935337], [7.0091070968248417, 12.541182676864368, 6.4536001581368456, 11.311144471240993, 14.091782136932949], [24.723148545826977, 2.4835752400911666, 0.71991251780297238, 18.251107733692681, 5.229072502586205], [2.0814069720651753, 7.3018497768134702, 4.9424996814325945, 18.626113958208684, 18.45494615148008], [13.609220852068022, 8.9597172068663582, 0.83033189363920101, 0.50978663653082368, 27.497759950895588], [1.9459130963029889, 7.85443098611489, 26.894316146072999, 5.1528264473248555, 9.5593298641842672], [21.233887860019429, 16.249029635198248, 12.952800620402915, 0.83079453437940654, 0.14030388999999999], [16.744580036444152, 2.1886347243503446, 13.889269956217021, 18.444027010742889, 0.14030481224558841], [2.0369739904562736, 23.248656704908274, 0.69146245372762138, 16.126292009569632, 9.303431381338207], [2.3044725227888692, 11.761845603525055, 0.85077508526742807, 9.1220894215139765, 27.367633906904672], [0.046247341601124148, 20.153985377689668, 12.264849483740601, 0.57265928787913667, 18.369075049089471], [16.573627024767106, 0.077907664570762786, 0.69187001749607435, 7.8891970175529416, 26.174214815613119], [23.883208552910478, 9.4237084272994984, 6.6840350729211933, 11.275469150721175, 0.14039533614765148], [0.046230847436216783, 10.339709859951595, 6.3938980995456971, 0.94510795490136101, 33.681869778165137], [22.87824211537227, 15.477203647737671, 10.709879441032186, 1.7171465383746682, 0.62434479748320393], [15.516320353660717, 31.691371073222477, 2.8336366281437413, 0.85695848369462446, 0.50853000127844361], [0.04623071000005672, 10.096009025683426, 9.6149141490230754, 12.767083620239058, 18.882579035054388], [0.046288530033982672, 19.351492443825173, 0.6913267686767719, 9.1804574286854805, 22.137251368778596], [13.360566002835867, 5.7500482702650855, 9.860677195959898, 5.4543943894892539, 16.981130681449891], [9.3721288161014975, 11.5942860765412, 7.4589494095420665, 9.6902050267617685, 13.291247211053467], [13.680276632802283, 2.8008434045689681, 6.6277617514383618, 20.230300874632309, 8.067633876558082], [0.046668225141551514, 6.8358670643124091, 12.599262725452895, 15.444659155830591, 16.480359369262555], [1.1214123015487314, 3.365847806150319, 15.148735438712322, 16.132368021853289, 15.638452971735337], [6.0926951188872431, 9.676427807448384, 9.9066743441850171, 11.871710814458064, 13.859308455021289], [0.047409042180985329, 5.9076323148252143, 15.085672538884641, 8.2917715604820295, 22.074331083627129]]

calculate_new_alpha(alpha,gamma_matrix,20)
alpha_nr(np.copy(gamma_matrix),init_alpha=alpha,maxit=20)