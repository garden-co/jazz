import { clsx } from "clsx";

export function TilescapeLogo(props: React.SVGProps<SVGSVGElement>) {
    return (
        <svg
            {...props}
            className={clsx(props.className, "text-black dark:text-white")}
            width="190"
            viewBox="0 0 646 164"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
        >
            <g clipPath="url(#clip0_12846_594)">
                <path
                    d="M233.529 123C229.789 123.733 226.123 124.045 222.529 123.935C218.936 123.825 215.728 123.128 212.904 121.845C210.081 120.562 207.954 118.545 206.524 115.795C205.241 113.338 204.544 110.845 204.434 108.315C204.361 105.748 204.324 102.852 204.324 99.625V47.1H215.874V99.075C215.874 101.458 215.893 103.53 215.929 105.29C216.003 107.05 216.388 108.535 217.084 109.745C218.404 112.018 220.494 113.32 223.354 113.65C226.251 113.943 229.643 113.815 233.529 113.265V123ZM192.939 72.84V63.6H233.529V72.84H192.939ZM241.103 53.865V42.7H252.598V53.865H241.103ZM241.103 123V63.6H252.598V123H241.103ZM264.164 123V42.15H275.659V123H264.164ZM312.471 124.65C306.567 124.65 301.379 123.367 296.906 120.8C292.469 118.197 289.004 114.585 286.511 109.965C284.054 105.308 282.826 99.9183 282.826 93.795C282.826 87.305 284.036 81.6767 286.456 76.91C288.912 72.1433 292.322 68.4583 296.686 65.855C301.049 63.2517 306.127 61.95 311.921 61.95C317.971 61.95 323.122 63.3617 327.376 66.185C331.629 68.9717 334.801 72.95 336.891 78.12C339.017 83.29 339.861 89.4317 339.421 96.545H327.926V92.365C327.852 85.4717 326.532 80.375 323.966 77.075C321.436 73.775 317.567 72.125 312.361 72.125C306.604 72.125 302.277 73.94 299.381 77.57C296.484 81.2 295.036 86.4433 295.036 93.3C295.036 99.8267 296.484 104.887 299.381 108.48C302.277 112.037 306.457 113.815 311.921 113.815C315.514 113.815 318.612 113.008 321.216 111.395C323.856 109.745 325.909 107.398 327.376 104.355L338.651 107.93C336.341 113.247 332.839 117.372 328.146 120.305C323.452 123.202 318.227 124.65 312.471 124.65ZM291.296 96.545V87.58H333.701V96.545H291.296ZM368.425 124.595C361.202 124.595 355.317 123 350.77 119.81C346.224 116.62 343.437 112.128 342.41 106.335L354.18 104.52C354.914 107.6 356.6 110.038 359.24 111.835C361.917 113.595 365.235 114.475 369.195 114.475C372.789 114.475 375.594 113.742 377.61 112.275C379.664 110.808 380.69 108.792 380.69 106.225C380.69 104.722 380.324 103.512 379.59 102.595C378.894 101.642 377.409 100.743 375.135 99.9C372.862 99.0567 369.397 98.0117 364.74 96.765C359.644 95.445 355.592 94.0333 352.585 92.53C349.615 90.99 347.489 89.2117 346.205 87.195C344.959 85.1417 344.335 82.6667 344.335 79.77C344.335 76.1767 345.289 73.0417 347.195 70.365C349.102 67.6883 351.779 65.6167 355.225 64.15C358.709 62.6833 362.779 61.95 367.435 61.95C371.982 61.95 376.034 62.665 379.59 64.095C383.147 65.525 386.025 67.56 388.225 70.2C390.425 72.8033 391.745 75.865 392.185 79.385L380.415 81.53C380.012 78.67 378.674 76.415 376.4 74.765C374.127 73.115 371.175 72.2167 367.545 72.07C364.062 71.9233 361.239 72.51 359.075 73.83C356.912 75.1133 355.83 76.8917 355.83 79.165C355.83 80.485 356.234 81.6033 357.04 82.52C357.884 83.4367 359.497 84.3167 361.88 85.16C364.264 86.0033 367.784 87.0117 372.44 88.185C377.427 89.4683 381.387 90.8983 384.32 92.475C387.254 94.015 389.344 95.8667 390.59 98.03C391.874 100.157 392.515 102.742 392.515 105.785C392.515 111.652 390.37 116.253 386.08 119.59C381.827 122.927 375.942 124.595 368.425 124.595ZM424.045 124.65C417.958 124.65 412.788 123.293 408.535 120.58C404.282 117.867 401.018 114.145 398.745 109.415C396.508 104.685 395.372 99.3133 395.335 93.3C395.372 87.1767 396.545 81.7683 398.855 77.075C401.165 72.345 404.465 68.6417 408.755 65.965C413.045 63.2883 418.197 61.95 424.21 61.95C430.7 61.95 436.237 63.5633 440.82 66.79C445.44 70.0167 448.483 74.435 449.95 80.045L438.51 83.345C437.373 80.0083 435.503 77.4233 432.9 75.59C430.333 73.72 427.382 72.785 424.045 72.785C420.268 72.785 417.17 73.6833 414.75 75.48C412.33 77.24 410.533 79.66 409.36 82.74C408.187 85.82 407.582 89.34 407.545 93.3C407.582 99.4233 408.975 104.373 411.725 108.15C414.512 111.927 418.618 113.815 424.045 113.815C427.748 113.815 430.737 112.972 433.01 111.285C435.32 109.562 437.08 107.105 438.29 103.915L449.95 106.665C448.007 112.458 444.798 116.913 440.325 120.03C435.852 123.11 430.425 124.65 424.045 124.65ZM471.128 124.65C466.728 124.65 463.043 123.843 460.073 122.23C457.103 120.58 454.848 118.417 453.308 115.74C451.804 113.027 451.053 110.057 451.053 106.83C451.053 103.823 451.584 101.183 452.648 98.91C453.711 96.6367 455.288 94.7117 457.378 93.135C459.468 91.5217 462.034 90.22 465.078 89.23C467.718 88.46 470.706 87.7817 474.043 87.195C477.379 86.6083 480.881 86.0583 484.548 85.545C488.251 85.0317 491.918 84.5183 495.548 84.005L491.368 86.315C491.441 81.6583 490.451 78.2117 488.398 75.975C486.381 73.7017 482.898 72.565 477.948 72.565C474.831 72.565 471.971 73.2983 469.368 74.765C466.764 76.195 464.949 78.5783 463.923 81.915L453.198 78.615C454.664 73.5183 457.451 69.4667 461.558 66.46C465.701 63.4533 471.201 61.95 478.058 61.95C483.374 61.95 487.994 62.8667 491.918 64.7C495.878 66.4967 498.774 69.3567 500.608 73.28C501.561 75.2233 502.148 77.2767 502.368 79.44C502.588 81.6033 502.698 83.9317 502.698 86.425V123H492.523V109.415L494.503 111.175C492.046 115.722 488.911 119.113 485.098 121.35C481.321 123.55 476.664 124.65 471.128 124.65ZM473.163 115.245C476.426 115.245 479.231 114.677 481.578 113.54C483.924 112.367 485.813 110.882 487.243 109.085C488.673 107.288 489.608 105.418 490.048 103.475C490.671 101.715 491.019 99.735 491.093 97.535C491.203 95.335 491.258 93.575 491.258 92.255L494.998 93.63C491.368 94.18 488.068 94.675 485.098 95.115C482.128 95.555 479.433 95.995 477.013 96.435C474.629 96.8383 472.503 97.3333 470.633 97.92C469.056 98.47 467.644 99.13 466.398 99.9C465.188 100.67 464.216 101.605 463.483 102.705C462.786 103.805 462.438 105.143 462.438 106.72C462.438 108.26 462.823 109.69 463.593 111.01C464.363 112.293 465.536 113.32 467.113 114.09C468.689 114.86 470.706 115.245 473.163 115.245ZM538.815 124.65C533.131 124.65 528.365 123.275 524.515 120.525C520.665 117.738 517.75 113.98 515.77 109.25C513.79 104.52 512.8 99.185 512.8 93.245C512.8 87.305 513.771 81.97 515.715 77.24C517.695 72.51 520.591 68.7883 524.405 66.075C528.255 63.325 532.985 61.95 538.595 61.95C544.168 61.95 548.971 63.325 553.005 66.075C557.075 68.7883 560.21 72.51 562.41 77.24C564.61 81.9333 565.71 87.2683 565.71 93.245C565.71 99.185 564.61 104.538 562.41 109.305C560.246 114.035 557.148 117.775 553.115 120.525C549.118 123.275 544.351 124.65 538.815 124.65ZM510.875 149.4V63.6H521.105V106.335H522.425V149.4H510.875ZM537.22 114.255C540.886 114.255 543.911 113.32 546.295 111.45C548.715 109.58 550.511 107.068 551.685 103.915C552.895 100.725 553.5 97.1683 553.5 93.245C553.5 89.3583 552.895 85.8383 551.685 82.685C550.511 79.5317 548.696 77.02 546.24 75.15C543.783 73.28 540.648 72.345 536.835 72.345C533.241 72.345 530.271 73.225 527.925 74.985C525.615 76.745 523.891 79.2017 522.755 82.355C521.655 85.5083 521.105 89.1383 521.105 93.245C521.105 97.3517 521.655 100.982 522.755 104.135C523.855 107.288 525.596 109.763 527.98 111.56C530.363 113.357 533.443 114.255 537.22 114.255ZM598.148 124.65C592.245 124.65 587.057 123.367 582.583 120.8C578.147 118.197 574.682 114.585 572.188 109.965C569.732 105.308 568.503 99.9183 568.503 93.795C568.503 87.305 569.713 81.6767 572.133 76.91C574.59 72.1433 578 68.4583 582.363 65.855C586.727 63.2517 591.805 61.95 597.598 61.95C603.648 61.95 608.8 63.3617 613.053 66.185C617.307 68.9717 620.478 72.95 622.568 78.12C624.695 83.29 625.538 89.4317 625.098 96.545H613.603V92.365C613.53 85.4717 612.21 80.375 609.643 77.075C607.113 73.775 603.245 72.125 598.038 72.125C592.282 72.125 587.955 73.94 585.058 77.57C582.162 81.2 580.713 86.4433 580.713 93.3C580.713 99.8267 582.162 104.887 585.058 108.48C587.955 112.037 592.135 113.815 597.598 113.815C601.192 113.815 604.29 113.008 606.893 111.395C609.533 109.745 611.587 107.398 613.053 104.355L624.328 107.93C622.018 113.247 618.517 117.372 613.823 120.305C609.13 123.202 603.905 124.65 598.148 124.65ZM576.973 96.545V87.58H619.378V96.545H576.973Z"
                    fill="currentColor"
                ></path>
                <path
                    fillRule="evenodd"
                    clipRule="evenodd"
                    d="M49.7396 136.076C41.2039 133.367 33.208 129.509 26.21 124.503C18.5964 119.055 12.8839 112.763 9.0724 106.048L17.5525 99.9807C25.2202 94.4948 29.0541 91.7518 33.8182 91.7518C38.5823 91.7519 42.4161 94.4948 50.0838 99.9808L58.8711 106.268C66.5387 111.754 70.3726 114.497 70.3726 117.905C70.3726 121.314 66.5387 124.057 58.871 129.542L50.0837 135.829C49.9681 135.912 49.8534 135.994 49.7396 136.076ZM103.928 138.504C89.2853 141.647 73.6068 141.651 58.9614 138.515L65.1867 134.061C72.8544 128.575 76.6883 125.832 81.4524 125.832C86.2165 125.832 90.0503 128.575 97.718 134.061L103.928 138.504ZM153.547 106.434C149.737 113.004 144.098 119.16 136.63 124.503C129.488 129.613 121.307 133.526 112.572 136.242C112.383 136.106 112.191 135.969 111.996 135.829L103.209 129.542C95.5412 124.056 91.7073 121.314 91.7073 117.905C91.7073 114.496 95.5412 111.754 103.209 106.268L111.996 99.9807C119.664 94.4948 123.498 91.7518 128.262 91.7518C133.026 91.7519 136.86 94.4948 144.528 99.9807L153.315 106.268C153.393 106.323 153.47 106.379 153.547 106.434ZM156.216 68.9348C160.467 79.0984 160.59 89.9575 156.584 100.161L150.843 96.0536C143.175 90.5677 139.341 87.8247 139.341 84.4162C139.341 81.0077 143.175 78.2647 150.843 72.7788L156.216 68.9348ZM110.833 33.2396C120.22 35.9665 129.016 40.0539 136.63 45.5017C143.628 50.5078 149.019 56.2277 152.805 62.3337L144.526 68.2571C136.858 73.743 133.024 76.4859 128.26 76.4859C123.496 76.4859 119.662 73.7429 111.994 68.257L103.207 61.97C95.5394 56.4841 91.7056 53.7411 91.7056 50.3326C91.7056 46.9241 95.5394 44.1812 103.207 38.6953L110.833 33.2396ZM60.231 31.2261C74.0863 28.4414 88.8062 28.4448 102.66 31.2362L97.7178 34.7718C90.0501 40.2577 86.2163 43.0006 81.4522 43.0006C76.6881 43.0006 72.8543 40.2576 65.1866 34.7717L60.231 31.2261ZM51.4673 33.3983L58.8711 38.6953C66.5387 44.1813 70.3726 46.9242 70.3726 50.3327C70.3726 53.7413 66.5387 56.4842 58.871 61.9701L50.0837 68.2571C42.4159 73.743 38.5821 76.4859 33.818 76.4859C29.0539 76.4859 25.2201 73.7429 17.5524 68.257L17.5524 68.257L9.80308 62.7127C13.5994 56.4643 19.0683 50.6112 26.21 45.5017C33.6782 40.1585 42.2829 36.1241 51.4673 33.3983ZM6.2707 100.197C2.24529 89.9712 2.36829 79.085 6.63968 68.8992L12.0624 72.7789C19.7301 78.2648 23.5639 81.0078 23.5639 84.4163C23.5639 87.8248 19.73 90.5678 12.0623 96.0537L6.2707 100.197ZM44.4836 84.1194C44.4835 87.5279 48.3174 90.2709 55.9851 95.7568L64.7723 102.044C72.44 107.53 76.2739 110.273 81.038 110.273C85.8021 110.273 89.6359 107.53 97.3036 102.044L106.091 95.7569C113.759 90.271 117.593 87.528 117.593 84.1195C117.593 80.711 113.759 77.968 106.091 72.4821L97.3038 66.1952C89.6361 60.7092 85.8022 57.9663 81.0381 57.9662C76.274 57.9662 72.4402 60.7092 64.7725 66.1951L55.9851 72.482C48.3174 77.9679 44.4836 80.7109 44.4836 84.1194Z"
                    fill="#FF601C"
                ></path>
            </g>
            <defs>
                <clipPath id="clip0_12846_594">
                    <rect width="646" height="164" fill="white"></rect>
                </clipPath>
            </defs>
        </svg>
    );
}