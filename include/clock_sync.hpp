#ifndef CLOCK_SYNC_HPP
#define CLOCK_SYNC_HPP

#include <cstdint>
#include <atomic>
#include <chrono>
#include <cmath>
#include <algorithm>
#include <array>
#include <system_error>

namespace distributed_token_bucket {

enum class ClockSyncError {
    Success = 0,
    ExcessiveDrift,
    ClockSkewOutOfBounds,
    KalmanUpdateFailure,
    InvalidMonotonicSample
};

class ClockSyncErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "ClockSyncError";
    }

    std::string message(int ev) const override {
        switch (static_cast<ClockSyncError>(ev)) {
            case ClockSyncError::Success:
                return "Success";
            case ClockSyncError::ExcessiveDrift:
                return "Clock drift exceeds maximum safety bound";
            case ClockSyncError::ClockSkewOutOfBounds:
                return "Clock skew violates $\\pm 50\\text{ms}$ invariant";
            case ClockSyncError::KalmanUpdateFailure:
                return "Kalman filter divergence detected";
            case ClockSyncError::InvalidMonotonicSample:
                return "Monotonic time sample regression detected";
            default:
                return "Unknown clock sync error";
            }
    }
};

inline const std::error_category& get_clock_sync_error_category() noexcept {
    static ClockSyncErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ClockSyncError e) noexcept {
    return {static_cast<int>(e), get_clock_sync_error_category()};
}

} // namespace distributed_token_bucket

namespace std {
template <>
struct is_error_code_enum<distributed_token_bucket::ClockSyncError> : true_type {};
} // namespace std

namespace distributed_token_bucket {

struct alignas(64) HybridLogicalClockState {
    std::atomic<uint64_t> physical_time_ns{0};
    std::atomic<uint32_t> logical_counter{0};
    std::atomic<int64_t> estimated_offset_ns{0};
    std::atomic<uint64_t> uncertainty_bound_ns{50000000ULL}; // 50ms default bound
};

class KalmanDriftEstimator {
private:
    double x_estimate_[2]; // [0]: clock offset (ns), [1]: drift rate (ns/ns)
    double p_cov_[2][2];   // Estimation error covariance matrix
    double q_process_[2];  // Process noise covariance
    double r_measurement_; // Measurement noise variance
    bool initialized_;

public:
    KalmanDriftEstimator() noexcept 
        : r_measurement_(1000000.0) // 1ms^2 measurement noise variance equivalent
        , initialized_(false) {
        x_estimate_[0] = 0.0;
        x_estimate_[1] = 0.0;
        
        p_cov_[0][0] = 1000.0;
        p_cov_[0][1] = 0.0;
        p_cov_[1][0] = 0.0;
        p_cov_[1][1] = 1.0;

        q_process_[0] = 10.0;
        q_process_[1] = 0.001;
    }

    ~KalmanDriftEstimator() = default;

    std::error_code update(int64_t measured_offset_ns, double dt_seconds) noexcept {
        if (dt_seconds <= 0.0) {
            return make_error_code(ClockSyncError::InvalidMonotonicSample);
        }

        if (!initialized_) {
            x_estimate_[0] = static_cast<double>(measured_offset_ns);
            x_estimate_[1] = 0.0;
            p_cov_[0][0] = 100.0;
            p_cov_[0][1] = 0.0;
            p_cov_[1][0] = 0.0;
            p_cov_[1][1] = 1.0;
            initialized_ = true;
            return make_error_code(ClockSyncError::Success);
        }

        // 1. Predict Step
        double x_pred_0 = x_estimate_[0] + x_estimate_[1] * dt_seconds;
        double x_pred_1 = x_estimate_[1];

        double p_pred_00 = p_cov_[0][0] + dt_seconds * (p_cov_[1][0] + p_cov_[0][1]) + dt_seconds * dt_seconds * p_cov_[1][1] + q_process_[0];
        double p_pred_01 = p_cov_[0][1] + dt_seconds * p_cov_[1][1];
        double p_pred_10 = p_cov_[1][0] + dt_seconds * p_cov_[1][1];
        double p_pred_11 = p_cov_[1][1] + q_process_[1];

        // 2. Update Step (Innovation)
        double innovation = static_cast<double>(measured_offset_ns) - x_pred_0;
        double s_innovation_cov = p_pred_00 + r_measurement_;

        if (s_innovation_cov <= 0.0) {
            return make_error_code(ClockSyncError::KalmanUpdateFailure);
        }

        double k_gain_0 = p_pred_00 / s_innovation_cov;
        double k_gain_1 = p_pred_10 / s_innovation_cov;

        x_estimate_[0] = x_pred_0 + k_gain_0 * innovation;
        x_estimate_[1] = x_pred_1 + k_gain_1 * innovation;

        p_cov_[0][0] = (1.0 - k_gain_0) * p_pred_00;
        p_cov_[0][1] = (1.0 - k_gain_0) * p_pred_01;
        p_cov_[1][0] = p_pred_10 - k_gain_1 * p_pred_00;
        p_cov_[1][1] = p_pred_11 - k_gain_1 * p_pred_01;

        return make_error_code(ClockSyncError::Success);
    }

    double get_estimated_offset() const noexcept {
        return x_estimate_[0];
    }

    double get_estimated_drift_rate() const noexcept {
        return x_estimate_[1];
    }

    double get_uncertainty_variance() const noexcept {
        return p_cov_[0][0];
    }
};

class alignas(64) ClockSyncManager {
private:
    HybridLogicalClockState hlc_state_;
    KalmanDriftEstimator kalman_estimator_;
    uint64_t max_allowed_skew_ns_;
    std::atomic<uint64_t> last_monotonic_ns_;

    uint64_t get_current_monotonic_ns() const noexcept {
        auto now = std::chrono::steady_clock::now();
        auto duration = now.time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
    }

    uint64_t get_current_system_ns() const noexcept {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
    }

public:
    explicit ClockSyncManager(uint64_t max_skew_ns = 50000000ULL) noexcept 
        : max_allowed_skew_ns_(max_skew_ns) {
        uint64_t initial_sys = get_current_system_ns();
        uint64_t initial_mono = get_current_monotonic_ns();
        
        hlc_state_.physical_time_ns.store(initial_sys, std::memory_order_relaxed);
        hlc_state_.logical_counter.store(0, std::memory_order_relaxed);
        hlc_state_.estimated_offset_ns.store(0, std::memory_order_relaxed);
        hlc_state_.uncertainty_bound_ns.store(max_skew_ns, std::memory_order_relaxed);
        
        last_monotonic_ns_.store(initial_mono, std::memory_order_relaxed);
    }

    ~ClockSyncManager() = default;

    std::error_code synchronize(int64_t remote_time_ns, int64_t propagation_delay_ns) noexcept {
        uint64_t current_mono = get_current_monotonic_ns();
        uint64_t last_mono = last_monotonic_ns_.load(std::memory_order_relaxed);
        
        if (current_mono < last_mono) {
            return make_error_code(ClockSyncError::InvalidMonotonicSample);
        }

        double dt_seconds = static_cast<double>(current_mono - last_mono) / 1e9;
        last_monotonic_ns_.store(current_mono, std::memory_order_relaxed);

        uint64_t local_sys = get_current_system_ns();
        int64_t measured_offset = remote_time_ns - (static_cast<int64_t>(local_sys) + propagation_delay_ns);

        if (std::abs(measured_offset) > static_cast<int64_t>(max_allowed_skew_ns_)) {
            return make_error_code(ClockSyncError::ClockSkewOutOfBounds);
        }

        std::error_code ec = kalman_estimator_.update(measured_offset, dt_seconds);
        if (ec) {
            return ec;
        }

        double est_offset = kalman_estimator_.get_estimated_offset();
        double est_variance = kalman_estimator_.get_uncertainty_variance();

        hlc_state_.estimated_offset_ns.store(static_cast<int64_t>(est_offset), std::memory_order_release);
        
        uint64_t calculated_uncertainty = static_cast<uint64_t>(std::sqrt(std::max(0.0, est_variance))) + static_cast<uint64_t>(propagation_delay_ns);
        hlc_state_.uncertainty_bound_ns.store(std::min(calculated_uncertainty, max_allowed_skew_ns_), std::memory_order_release);

        return make_error_code(ClockSyncError::Success);
    }

    uint64_t get_adjusted_timestamp_ns() noexcept {
        uint64_t physical_now = get_current_system_ns();
        int64_t offset = hlc_state_.estimated_offset_ns.load(std::memory_order_acquire);
        
        uint64_t adjusted = static_cast<uint64_t>(static_cast<int64_t>(physical_now) + offset);
        
        uint64_t last_pt = hlc_state_.physical_time_ns.load(std::memory_order_relaxed);
        uint32_t logical = hlc_state_.logical_counter.load(std::memory_order_relaxed);

        while (true) {
            if (adjusted > last_pt) {
                if (hlc_state_.physical_time_ns.compare_exchange_weak(last_pt, adjusted, std::memory_order_release, std::memory_order_relaxed)) {
                    hlc_state_.logical_counter.store(0, std::memory_order_relaxed);
                    return adjusted;
                }
            } else if (adjusted == last_pt) {
                uint32_t new_logical = logical + 1;
                if (hlc_state_.logical_counter.compare_exchange_weak(logical, new_logical, std::memory_order_release, std::memory_order_relaxed)) {
                    return last_pt;
                }
            } else {
                uint32_t new_logical = logical + 1;
                if (hlc_state_.logical_counter.compare_exchange_weak(logical, new_logical, std::memory_order_release, std::memory_order_relaxed)) {
                    return last_pt;
                }
            }
            last_pt = hlc_state_.physical_time_ns.load(std::memory_order_relaxed);
            logical = hlc_state_.logical_counter.load(std::memory_order_relaxed);
        }
    }

    uint64_t get_uncertainty_bound_ns() const noexcept {
        return hlc_state_.uncertainty_bound_ns.load(std::memory_order_acquire);
    }

    bool verify_safety_invariant(uint64_t peer_timestamp_ns, uint64_t local_timestamp_ns) const noexcept {
        uint64_t uncertainty = hlc_state_.uncertainty_bound_ns.load(std::memory_order_acquire);
        int64_t diff = static_cast<int64_t>(peer_timestamp_ns) - static_cast<int64_t>(local_timestamp_ns);
        return std::abs(diff) <= static_cast<int64_t>(uncertainty + max_allowed_skew_ns_);
    }
};

} // namespace distributed_token_bucket

#endif // CLOCK_SYNC_HPP