"use client";

import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";

interface FormData {
  appName: string;
  description: string;
  projectUrl: string;
  repo: string;
  preferredCommunication: string;
  handle: string;
  message: string;
  nickName?: string; // bot protection, hidden, should be left empty by actual user
}

interface FormErrors {
  appName?: string;
  handle?: string;
  description?: string;
}

const defaultFormData: FormData = {
  appName: "",
  description: "",
  projectUrl: "",
  repo: "",
  preferredCommunication: "email",
  handle: "",
  message: "",
};

function FieldError({ message }: { message?: string }) {
  return message ? (
    <p className="mt-1 text-sm text-red-600">{message}</p>
  ) : null;
}

export function ContactForm() {
  const [formData, setFormData] = useState<FormData>(defaultFormData);
  const [errors, setErrors] = useState<FormErrors>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState<
    "idle" | "success" | "error"
  >("idle");
  const [submitMessage, setSubmitMessage] = useState("");

  const validateForm = (): boolean => {
    const newErrors: FormErrors = {};

    if (!formData.appName.trim()) {
      newErrors.appName = "App name is required";
    }

    if (!formData.handle.trim()) {
      newErrors.handle = "Method of communication is required";
    } else if (
      formData.preferredCommunication === "email" &&
      !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.handle)
    ) {
      newErrors.handle = "Please enter a valid email address";
    }

    if (!formData.description.trim()) {
      newErrors.description = "Description is required";
    }

    // Don't validate nickName, it's just the spam trap
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    setIsSubmitting(true);
    setSubmitStatus("idle");
    const trimmedData: FormData = {
      ...formData,
      appName: formData.appName.trim(),
      description: formData.description.trim(),
      handle: formData.handle.trim(),
      projectUrl: formData.projectUrl.trim(),
      repo: formData.repo.trim(),
      message: formData.message.trim(),
    };

    try {
      const response = await fetch("/api/contact", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(trimmedData),
      });

      const result = await response.json();

      if (response.ok) {
        setSubmitStatus("success");
        setSubmitMessage(result.message);
        // Reset form on success
        setFormData(defaultFormData);
      } else {
        setSubmitStatus("error");
        setSubmitMessage(
          result.error || "Something went wrong. Please try again.",
        );
      }
    } catch (error) {
      setSubmitStatus("error");
      setSubmitMessage(
        "Network error. Please check your connection and try again.",
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleInputChange = (field: keyof FormData, value: string) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
    // Clear error when user starts typing
    if (errors[field as keyof FormErrors]) {
      setErrors((prev) => ({ ...prev, [field]: undefined }));
    }
  };

  return (
    <div className="bg-stone dark:bg-stone mx-auto rounded-lg border p-6 shadow-sm">
      <HeroHeader
        level="h2"
        title="Submit a Project"
        slogan="We'd love to hear more about your Jazz app. Please fill out the form below and we'll get back to you as soon as possible."
        className="pt-0"
        pt={false}
      />
      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <div>
            <Label htmlFor="appName">App Name *</Label>
            <Input
              id="appName"
              type="text"
              value={formData.appName}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleInputChange("appName", e.target.value)
              }
              placeholder="The name of your app"
              error={!!errors.appName}
            />
            {errors.appName && <FieldError message={errors.appName} />}
          </div>
          <div>
            <Label htmlFor="description">Description *</Label>
            <Input
              id="description"
              type="text"
              value={formData.description}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleInputChange("description", e.target.value)
              }
              placeholder="Brief description of your app"
              error={!!errors.description}
            />
            {errors.description && <FieldError message={errors.description} />}
          </div>
        </div>

        <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <div>
            <Label htmlFor="contactMethod">Preferred Contact Method *</Label>
            <Select
              value={formData.preferredCommunication}
              onValueChange={(value) =>
                handleInputChange("preferredCommunication", value)
              }
            >
              <SelectTrigger id="contactMethod">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="email">Email</SelectItem>
                <SelectItem value="discord">Discord</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label htmlFor="handle">Email/Discord Handle *</Label>
            <Input
              id="handle"
              type={
                formData.preferredCommunication === "email" ? "email" : "text"
              }
              value={formData.handle}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleInputChange("handle", e.target.value)
              }
              placeholder={
                formData.preferredCommunication === "email"
                  ? "your.email@example.com"
                  : "your.discord.handle"
              }
              error={!!errors.handle}
            />
            {errors.handle && <FieldError message={errors.handle} />}
          </div>
        </div>

        <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <div>
            <Label htmlFor="projectUrl">Project URL</Label>
            <Input
              id="projectUrl"
              type="text"
              value={formData.projectUrl}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleInputChange("projectUrl", e.target.value)
              }
              placeholder="Your project url"
            />
          </div>
          <div>
            <Label htmlFor="repo">Project Repository (optional)</Label>
            <Input
              id="repo"
              type="text"
              value={formData.repo}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleInputChange("repo", e.target.value)
              }
              placeholder="Your project repo"
            />
          </div>
        </div>
        <div className="grid grid-cols-1 gap-6">
          <div>
            <Label htmlFor="message">Message (optional)</Label>
            <Textarea
              id="message"
              value={formData.message}
              onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
                handleInputChange("message", e.target.value)
              }
              placeholder="Anything else you'd like to add?"
              autoComplete="off"
              rows={5}
              className="min-h-24"
            />
          </div>
          {/* this is a bot protection field, hidden from the user */}
          <div className="hidden" aria-hidden="true">
            <label htmlFor="nickName">Nickname</label>
            <input
              id="nickName"
              name="nickName"
              type="text"
              tabIndex={-1}
              autoComplete="off"
              value={formData.nickName || ""}
              onChange={(e) =>
                handleInputChange("nickName" as keyof FormData, e.target.value)
              }
            />
          </div>
        </div>

        {submitStatus !== "idle" && (
          <div
            className={`rounded-md p-4 ${
              submitStatus === "success"
                ? "border border-green-200 bg-green-50 text-green-800 dark:border-green-800 dark:bg-green-900/20 dark:text-green-400"
                : "border border-red-200 bg-red-50 text-red-800 dark:border-red-800 dark:bg-red-900/20 dark:text-red-400"
            }`}
          >
            {submitMessage}
          </div>
        )}

        <div className="text-right">
          <Button
            type="submit"
            disabled={isSubmitting}
            variant="default"
            size="lg"
            className="w-full px-8 md:w-auto"
          >
            {isSubmitting ? "Sending..." : "Submit"}
          </Button>
        </div>
      </form>
    </div>
  );
}
