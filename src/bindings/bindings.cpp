// SPEC-3.10: pybind11 bindings exposing the replay + fault-injection + detector
// surface to Python so research/experiments (and the FastAPI status page,
// SPEC-3.11) can drive the REAL C++ pipeline rather than a Python
// reimplementation. This is offline/research scaffolding, NOT the hot path:
// pybind11 owns the objects, conversions allocate, and load_rule_config is
// allowed to throw (pybind translates std::runtime_error -> Python
// RuntimeError). See the pinned contract in python/tests/test_bindings.py.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <core/message.hpp>
#include <detect/baseline.hpp>
#include <detect/conformal.hpp>
#include <detect/cusum.hpp>
#include <detect/rules.hpp>
#include <replay/checksum.hpp>
#include <replay/fault_injector.hpp>
#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <cstddef>
#include <cstdint>

namespace py = pybind11;
using namespace telemetry;
using namespace telemetry::replay;
using namespace telemetry::detect;

namespace {

// Pull the next message out of a StreamReplayer, raising StopIteration when the
// stream is exhausted. This is the Python iteration protocol arm pinned by the
// tests (`for msg in replayer`); the Optional[Message] arm is intentionally not
// exposed.
Message replayer_next(StreamReplayer& r) {
    Message m{};
    if (!r.next(m)) {
        throw py::stop_iteration();
    }
    return m;
}

}  // namespace

PYBIND11_MODULE(telemetry_engine, m) {
    m.doc() = "Python bindings for the telemetry anomaly engine (SPEC 3.10).";

    // ---- core/message ----------------------------------------------------
    py::class_<Message>(m, "Message")
        .def(py::init([](std::uint32_t stream_id, std::int64_t ts_ns,
                         double value, std::uint64_t seq) {
                 Message msg{};
                 msg.stream_id = stream_id;
                 msg.ts_ns = ts_ns;
                 msg.value = value;
                 msg.seq = seq;
                 return msg;
             }),
             py::arg("stream_id"), py::arg("ts_ns"), py::arg("value"),
             py::arg("seq"))
        .def_readonly("stream_id", &Message::stream_id)
        .def_readonly("ts_ns", &Message::ts_ns)
        .def_readonly("value", &Message::value)
        .def_readonly("seq", &Message::seq);

    // ---- replay/recorder -------------------------------------------------
    py::class_<StreamRecorder>(m, "StreamRecorder")
        .def(py::init<const std::string&>(), py::arg("path"))
        .def("write", &StreamRecorder::write, py::arg("msg"))
        .def("is_open", &StreamRecorder::is_open)
        .def("count", &StreamRecorder::count)
        .def("close", &StreamRecorder::close);

    // ---- replay/replayer -------------------------------------------------
    py::class_<StreamReplayer> replayer(m, "StreamReplayer");
    py::enum_<StreamReplayer::Status>(replayer, "Status")
        .value("kOk", StreamReplayer::Status::kOk)
        .value("kFileNotFound", StreamReplayer::Status::kFileNotFound)
        .value("kBadMagic", StreamReplayer::Status::kBadMagic)
        .value("kUnsupportedVersion", StreamReplayer::Status::kUnsupportedVersion);
    replayer.def(py::init<const std::string&>(), py::arg("path"))
        .def_property_readonly("status", &StreamReplayer::status)
        .def_property_readonly("truncated", &StreamReplayer::truncated)
        .def_property_readonly("messages_replayed",
                               &StreamReplayer::messages_replayed)
        .def("__iter__", [](StreamReplayer& r) -> StreamReplayer& { return r; })
        .def("__next__", &replayer_next);

    // ---- replay/checksum -------------------------------------------------
    py::class_<StreamChecksum>(m, "StreamChecksum")
        .def(py::init<>())
        .def("update", &StreamChecksum::update, py::arg("msg"))
        .def("digest", &StreamChecksum::digest);

    // ---- replay/fault_injector -------------------------------------------
    py::enum_<FaultType>(m, "FaultType")
        .value("kSpike", FaultType::kSpike)
        .value("kDrift", FaultType::kDrift)
        .value("kStuckAtValue", FaultType::kStuckAtValue)
        .value("kDropout", FaultType::kDropout);

    m.attr("kAutoOnset") = py::int_(kAutoOnset);

    py::class_<FaultSpec>(m, "FaultSpec")
        .def(py::init([](FaultType type, std::uint32_t stream_id,
                         std::size_t onset_index, std::size_t duration,
                         double magnitude) {
                 return FaultSpec{type, stream_id, onset_index, duration,
                                  magnitude};
             }),
             py::arg("type"), py::arg("stream_id"), py::arg("onset_index"),
             py::arg("duration"), py::arg("magnitude"))
        .def_readonly("type", &FaultSpec::type)
        .def_readonly("stream_id", &FaultSpec::stream_id)
        .def_readonly("onset_index", &FaultSpec::onset_index)
        .def_readonly("duration", &FaultSpec::duration)
        .def_readonly("magnitude", &FaultSpec::magnitude);

    py::class_<FaultLabel>(m, "FaultLabel")
        .def_readonly("stream_id", &FaultLabel::stream_id)
        .def_readonly("type", &FaultLabel::type)
        .def_readonly("t_start_ns", &FaultLabel::t_start_ns)
        .def_readonly("t_end_ns", &FaultLabel::t_end_ns);

    py::class_<FaultInjector>(m, "FaultInjector")
        .def(py::init<std::uint64_t>(), py::arg("seed"))
        .def("inject", &FaultInjector::inject, py::arg("input_path"),
             py::arg("output_path"), py::arg("labels_path"), py::arg("specs"));

    m.def("read_labels", &read_labels, py::arg("labels_path"));

    // ---- detect/rules ----------------------------------------------------
    py::enum_<RuleResult>(m, "RuleResult")
        .value("kOk", RuleResult::kOk)
        .value("kOutOfBounds", RuleResult::kOutOfBounds)
        .value("kRateViolation", RuleResult::kRateViolation);

    py::class_<RuleConfig>(m, "RuleConfig")
        .def(py::init([](std::uint32_t stream_id, double min, double max,
                         double max_rate_of_change) {
                 return RuleConfig{stream_id, min, max, max_rate_of_change};
             }),
             py::arg("stream_id"), py::arg("min"), py::arg("max"),
             py::arg("max_rate_of_change"))
        .def_readonly("stream_id", &RuleConfig::stream_id)
        .def_readonly("min", &RuleConfig::min)
        .def_readonly("max", &RuleConfig::max)
        .def_readonly("max_rate_of_change", &RuleConfig::max_rate_of_change);

    // load_rule_config throws std::runtime_error on failure; pybind11
    // translates it to a Python RuntimeError automatically.
    m.def("load_rule_config", &load_rule_config, py::arg("path"));

    py::class_<RuleChecker>(m, "RuleChecker")
        .def(py::init<>())
        .def("add_rule", &RuleChecker::add_rule, py::arg("cfg"))
        .def("check", &RuleChecker::check, py::arg("stream_id"),
             py::arg("value"), py::arg("ts_ns"));

    // ---- detect/baseline -------------------------------------------------
    py::class_<EwmaBaseline>(m, "EwmaBaseline")
        .def(py::init<double>(), py::arg("alpha"))
        .def("update", &EwmaBaseline::update, py::arg("stream_id"),
             py::arg("value"))
        .def("zscore", &EwmaBaseline::zscore, py::arg("stream_id"),
             py::arg("value"))
        .def("mean", &EwmaBaseline::mean, py::arg("stream_id"))
        .def("variance", &EwmaBaseline::variance, py::arg("stream_id"));

    // ---- detect/cusum ----------------------------------------------------
    py::class_<CusumDetector>(m, "CusumDetector")
        .def(py::init<double, double, double, std::size_t>(), py::arg("alpha"),
             py::arg("k"), py::arg("h"), py::arg("warmup_n") = 1)
        .def("update_and_check", &CusumDetector::update_and_check,
             py::arg("stream_id"), py::arg("value"));

    // ---- detect/conformal ------------------------------------------------
    py::class_<ConformalThreshold>(m, "ConformalThreshold")
        .def(py::init<std::size_t>(), py::arg("window_capacity"))
        .def("update", &ConformalThreshold::update, py::arg("stream_id"),
             py::arg("score"))
        .def("threshold", &ConformalThreshold::threshold, py::arg("stream_id"),
             py::arg("alpha"))
        .def("is_anomalous", &ConformalThreshold::is_anomalous,
             py::arg("stream_id"), py::arg("score"), py::arg("alpha"));
}
